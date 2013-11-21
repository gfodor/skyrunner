module SkyRunner::Job
  attr_accessor :skyrunner_job_id

  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def on_completed(methods)
      add_job_event_methods(methods, :completed)
    end

    def on_failed(methods)
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

  def execute!(args = {})
    job_id = SecureRandom.hex
    self.skyrunner_job_id = job_id

    table = SkyRunner.dynamo_db_table
    queue = SkyRunner.sqs_queue

    record = table.items.put(job_id: job_id, namespace: SkyRunner.job_namespace, class: self.class.name, args: args.to_json, total_tasks: 1, completed_tasks: 0, done: 0, failed: 0)

    pending_args = []

    flush = lambda do
      messages = pending_args.map do |task_args|
        { job_id: job_id, task_id: SecureRandom.hex, task_args: task_args }.to_json
      end

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

      record.attributes.add({ total_tasks: messages.size - dropped_message_count })
    end

    self.run(args) do |*task_args|
      pending_args << task_args

      if pending_args.size >= SkyRunner::SQS_BATCH_SIZE
        1.upto(5) do
          flush.()
          sleep 5 if pending_args.size > 0
        end
      end
    end

    1.upto(5) do
      flush.() if pending_args.size > 0
      sleep 5 if pending_args.size > 0
    end

    handle_task_completed!
  end

  def consume!(task_args)
    begin
      self.send(task_args[0].to_sym, task_args[1].symbolize_keys)
      handle_task_completed!
    rescue Exception => e
      handle_task_failed! rescue nil
      raise
    end
  end

  private 

  def dynamo_db_record
    SkyRunner.dynamo_db_table.items[self.skyrunner_job_id]
  end

  def handle_task_failed!
    return false unless self.skyrunner_job_id

    begin
      record = dynamo_db_record
      record.attributes.add({ failed: 1 })

      (self.class.job_event_methods[:failed] || []).each do |method|
        if self.method(method).arity == 0 && self.method(method).parameters.size == 0
          self.send(method)
        else
          self.send(method, JSON.parse(record.attributes["args"]).symbolize_keys)
        end
      end
    rescue Exception => e
    end
  end

  def handle_task_completed!
    return false unless self.skyrunner_job_id

    record = dynamo_db_record

    new_attributes = record.attributes.add({ completed_tasks: 1 }, return: :all_new)

    if new_attributes["total_tasks"] == new_attributes["completed_tasks"]
      begin
        if_condition = { completed_tasks: new_attributes["total_tasks"], done: 0 }

        record.attributes.update(if: if_condition) do |u|
          u.add(done: 1)
        end

        (self.class.job_event_methods[:completed] || []).each do |method|
          if self.method(method).arity == 0 && self.method(method).parameters.size == 0
            self.send(method)
          else
            self.send(method, JSON.parse(record.attributes["args"]).symbolize_keys)
          end
        end
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException => e
        # This is OK, we had a double finisher.
      end
    end

    true
  end
end
