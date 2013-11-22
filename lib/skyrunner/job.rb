module SkyRunner::Job
  attr_accessor :skyrunner_job_id
  attr_accessor :skyrunner_job_is_solo

  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def on_completed(*methods)
      add_job_event_methods(methods, :completed)
    end

    def on_failed(*methods)
      add_job_event_methods(methods, :failed)
    end

    def job_event_methods
      @_job_event_methods ||= {}
    end

    private 

    def add_job_event_methods(methods, type)
      methods = Array(methods)
      job_event_methods[type] ||= []
      job_event_methods[type].concat(methods.map(&:to_sym))
    end
  end

  def is_solo?
    self.skyrunner_job_is_solo
  end

  def execute!(job_args = {})
    return execute_local!(job_args) if SkyRunner::run_locally

    job_id = SecureRandom.hex
    self.skyrunner_job_id = job_id

    table = nil
    record = nil

    queue = SkyRunner.sqs_queue
    pending_args = []
    fired_solo = false

    flush = lambda do
      messages = pending_args.map do |task_args|
        { job_id: job_id, task_id: SecureRandom.hex, job_args: job_args, task_args: task_args, job_class: self.class.name }
      end

      if record.nil?
        # Run an un-coordinated solo job if only one message
        if messages.size > 1
          SkyRunner::retry_dynamo_db do
            table = SkyRunner.dynamo_db_table
            record = table.items.put(id: job_id, created_at: Time.now.to_s, task_id: job_id, class: self.class.name, args: job_args.to_json, total_tasks: 1, completed_tasks: 0, done: 0, failed: 0)
          end
        else
          fired_solo = true

          messages.each do |m|
            m[:is_solo] = true
          end
        end
      end

      messages = messages.map(&:to_json)

      dropped_message_count = 0
      pending_args.clear

      begin
        queue.batch_send(messages)
      rescue AWS::SQS::Errors::BatchSendError => e
        dropped_message_count = e.errors.size

        # Re-add dropped args
        e.errors.each do |error|
          pending_args << JSON.parse(error[:message_body])["task_args"]
        end
      end

      if record
        SkyRunner::retry_dynamo_db do
          record.attributes.add({ total_tasks: messages.size - dropped_message_count })
        end
      end
    end

    self.run(job_args) do |*task_args|
      pending_args << task_args

      if pending_args.size >= SkyRunner::SQS_MAX_BATCH_SIZE
        1.upto(5) do
          flush.()
          sleep 5 if pending_args.size > 0
          break if pending_args.size == 0
        end
      end
    end

    1.upto(5) do
      flush.() if pending_args.size > 0
      sleep 5 if pending_args.size > 0
      break if pending_args.size == 0
    end

    unless fired_solo
      handle_task_completed!(job_args)
    end
  end

  def consume!(job_args, task_args)
    begin
      self.send(task_args[0].to_sym, task_args[1].symbolize_keys)
      handle_task_completed!(job_args)
    rescue Exception => e
      handle_task_failed!(job_args) rescue nil
      raise
    end
  end

  def fire_post_event_method(event_type, job_args)
    (self.class.job_event_methods[event_type] || []).each do |method|
      if self.method(method).arity == 0 && self.method(method).parameters.size == 0
        self.send(method)
      else
        self.send(method, job_args.symbolize_keys)
      end
    end
  end

  private 

  def execute_local!(job_args = {})
    job_id = SecureRandom.hex
    self.skyrunner_job_id = job_id
    task_arg_list = []

    self.run(job_args) do |*task_args|
      task_arg_list << task_args
    end

    task_arg_list.shuffle!

    task_arg_list.each_with_index do |task_args, task_index|
      task = self.class.new
      task.skyrunner_job_id = job_id

      begin
        task.send(task_args[0].to_sym, task_args[1].symbolize_keys)
      rescue Exception => e
        task.fire_post_event_method(:failed, job_args)
        raise e
        break
      end

      if task_index == task_arg_list.size - 1
        task.fire_post_event_method(:completed, job_args)
      end
    end
  end

  def dynamo_db_record
    SkyRunner.dynamo_db_table.items[self.skyrunner_job_id, self.skyrunner_job_id]
  end

  def handle_task_failed!(job_args)
    return false unless self.skyrunner_job_id

    begin
      unless is_solo?
        record = dynamo_db_record

        SkyRunner::retry_dynamo_db do
          record.attributes.add({ failed: 1 })
        end
      end

      self.fire_post_event_method(:failed, job_args)

      unless is_solo?
        delete_task_records! rescue nil
      end
    rescue Exception => e
    end
  end

  def handle_task_completed!(job_args)
    return false unless self.skyrunner_job_id

    unless is_solo?
      record = dynamo_db_record
      new_attributes = nil

      SkyRunner::retry_dynamo_db do
        new_attributes = record.attributes.add({ completed_tasks: 1 }, return: :all_new)
      end
    end

    if is_solo? || new_attributes["total_tasks"] == new_attributes["completed_tasks"]
      begin
        unless is_solo?
          if_condition = { completed_tasks: new_attributes["total_tasks"], done: 0 }

          SkyRunner::retry_dynamo_db do
            record.attributes.update(if: if_condition) do |u|
              u.add(done: 1)
              u.set(completed_at: Time.now.to_s)
            end
          end
        end

        self.fire_post_event_method(:completed, job_args)

        unless is_solo?
          delete_task_records! rescue nil
        end
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException => e
        # This is OK, we had a double finisher so lets block them.
      end
    end

    true
  end

  def delete_task_records!
    return if is_solo?

    delete_batch_queue = Queue.new
    mutex = Mutex.new
    delete_items_queued = false
    threads = []

    1.upto([1, (SkyRunner::consumer_threads / 4.0).floor].max) do 
      threads << Thread.new do

        db_table = SkyRunner.dynamo_db_table

        loop do
          should_break = false

          mutex.synchronize do
            should_break = (SkyRunner::stop_consuming? || delete_items_queued) && delete_batch_queue.empty?
          end

          break if should_break

          if delete_batch_queue.size > 0
            batch = delete_batch_queue.pop

            if batch
              SkyRunner::retry_dynamo_db do
                db_table.batch_delete(batch)
              end
            end
          else
            sleep 1
          end
        end
      end
    end

    items_to_delete = []
    table = SkyRunner.dynamo_db_table

    table.items.query(hash_value: "#{self.skyrunner_job_id}-tasks", select: [:id, :task_id]) do |task_item|
      items_to_delete << [task_item.attributes["id"], task_item.attributes["task_id"]]

      if items_to_delete.size >= 25
        delete_batch_queue << items_to_delete
        items_to_delete = []
      end
    end

    delete_batch_queue << items_to_delete unless items_to_delete.empty?
    
    mutex.synchronize do
      delete_items_queued = true
    end

    threads.each(&:join)
  end
end
