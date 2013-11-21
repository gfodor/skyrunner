require "skyrunner/version"
require "aws-sdk"
require "active_support"
require "active_support/core_ext"
require "log4r"
require "json"
require "set"

module SkyRunner
  require "skyrunner/engine" if defined?(Rails)

  SQS_MAX_BATCH_SIZE = 10 # Constant defined by AWS

  def self.setup
    yield self
  end

  def self.init!(params = {})
    table = self.dynamo_db_table

    if !table.exists? || params[:purge]
      table_name = SkyRunner.dynamo_db_table_name

      if table.exists? && params[:purge]
        SkyRunner.log :warn, "Purging DynamoDB table #{table_name}."
        table.delete

        sleep 1 while table.exists?
      end

      SkyRunner.log :info, "Creating DynamoDB table #{table_name}."

      table = dynamo_db.tables.create(table_name, 
                                     SkyRunner.dynamo_db_read_capacity, 
                                     SkyRunner.dynamo_db_write_capacity,
                                     hash_key: { job_id: :string })

      sleep 1 while table.status == :creating
    end

    queue = self.sqs_queue

    if !queue || params[:purge]
      queue_name = SkyRunner.sqs_queue_name

      if queue && params[:purge]
        SkyRunner.log :warn, "Purging SQS queue #{queue_name}. Waiting 65 seconds to re-create."
        queue.delete

        sleep 65
      end

      SkyRunner.log :info, "Creating SQS queue #{queue_name}."

      queue = sqs.queues.create(queue_name, 
                                visibility_timeout: SkyRunner.sqs_visibility_timeout,
                                message_retention_period: SkyRunner.sqs_message_retention_period)
    end

    true
  end

  def self.consume!(&block)
    queue = sqs_queue
    table = dynamo_db_table
    raise "Queue #{SkyRunner::sqs_queue_name} not found. Try running 'skyrunner init'" unless queue
    raise "DynamoDB table #{SkyRunner::dynamo_db_table_name} not found. Try running 'skyrunner init'" unless table && table.exists?

    local_queue = Queue.new
    error_queue = Queue.new

    threads = []

    1.upto(SkyRunner::num_threads) do
      threads << Thread.new do
        loop do
          break if SkyRunner::stop_consuming? && local_queue.size == 0

          sleep 1 unless local_queue.size > 0

          klass, job_id, task_args, message = local_queue.pop

          if klass
            SkyRunner::log :info, "Run Task: #{task_args} Job: #{job_id} Message: #{message.id}"

            job = klass.new
            job.skyrunner_job_id = job_id

            begin
              job.consume!(task_args)
              message.delete
            rescue Exception => e
              error_queue.push(e)
              SkyRunner::log :error, "Task Failed: #{task_args} Job: #{job_id} #{e.message} #{e.backtrace.join("\n")}"
            end
          end
        end
      end
    end

    log :info, "Consumer started."

    loop do
      if error_queue.size > 0
        SkyRunner::stop_consuming!

        while error_queue.size > 0
          error = error_queue.pop
          yield error if block_given?
        end
      end

      return true if stop_consuming?

      sleep 1 while local_queue.size >= SkyRunner.num_threads

      received_messages = []

      queue.receive_messages(limit: [1, [SkyRunner.consumer_batch_size, SQS_MAX_BATCH_SIZE].min].max, wait_time_seconds: 5) do |message|
        received_messages << [message, JSON.parse(message.body)]
      end

      next unless received_messages.size > 0

      table.batch_get(:all, received_messages.map { |m| m[1]["job_id"] }.uniq, consistent_read: true) do |record|
        received_messages.select { |m| m[1]["job_id"] == record["job_id"] }.each_with_index do |received_message|
          message = received_message[1]
          job_id = message["job_id"]

          if record["namespace"] == SkyRunner.job_namespace && record["failed"] == 0 && error_queue.size == 0
            start_time = Time.now

            begin
              klass = Kernel.const_get(record["class"])
              task_args = message["task_args"]
              local_queue.push([klass, job_id, task_args, received_message[0]])
            rescue NameError => e
              log :error, "Task Failed: No such class #{record["class"]} #{e.message}"
              yield e if block_given?
            end
          end
        end
      end
    end

    threads.each { |t| t.join }

    true
  end

  def self.dynamo_db_table
    dynamo_db.tables[SkyRunner.dynamo_db_table_name].tap do |table|
      table.load_schema if table && table.exists?
    end
  end

  def self.sqs_queue
    begin
      sqs.queues.named(SkyRunner.sqs_queue_name)
    rescue AWS::SQS::Errors::NonExistentQueue => e
      return nil
    end
  end

  def self.log(type, message)
    SkyRunner.logger.send(type, "[SkyRunner] #{message}")
  end

  mattr_accessor :dynamo_db_table_name
  @@dynamo_db_table_name = "skyrunner_jobs"

  mattr_accessor :dynamo_db_read_capacity
  @@dynamo_db_read_capacity = 10

  mattr_accessor :dynamo_db_write_capacity
  @@dynamo_db_write_capacity = 10

  mattr_accessor :sqs_queue_name
  @@sqs_queue_name = "skyrunner_tasks"

  mattr_accessor :sqs_visibility_timeout
  @@sqs_visibility_timeout = 90

  mattr_accessor :sqs_message_retention_period
  @@sqs_message_retention_period = 345600

  mattr_accessor :job_namespace
  @@job_namespace = "default"

  mattr_accessor :consumer_batch_size
  @@consumer_batch_size = 10

  mattr_accessor :logger
  @@logger = Log4r::Logger.new("skyrunner")

  mattr_accessor :num_threads
  @@num_threads = 10

  mattr_accessor :stop_consuming_flag

  @@stop_consuming_mutex = Mutex.new

  def self.stop_consuming?
    @@stop_consuming_mutex.synchronize do
      SkyRunner::stop_consuming_flag
    end
  end

  def self.stop_consuming!(its_a_trap=false)
    if its_a_trap
      SkyRunner::stop_consuming_flag = true
    else
      @@stop_consuming_mutex.synchronize do
        SkyRunner::stop_consuming_flag = true
      end
    end
  end

  private

  def self.dynamo_db
    @dynamo_db ||= AWS::DynamoDB.new
  end

  def self.sqs
    @sqs ||= AWS::SQS.new
  end
end

require "skyrunner/job"
