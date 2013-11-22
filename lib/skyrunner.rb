require "skyrunner/version"
require "aws-sdk"
require "active_support"
require "active_support/core_ext"
require "log4r"
require "json"
require "set"
require "retries"

module SkyRunner
  require "skyrunner/engine" if defined?(Rails)

  SQS_MAX_BATCH_SIZE = 10 # Constant defined by AWS

  def self.setup
    yield self
  end

  def self.init!(params = {})
    table = self.dynamo_db_table

    if !table.exists? || params[:purge]
      table_name = SkyRunner::dynamo_db_table_name

      if table.exists? && params[:purge]
        SkyRunner.log :warn, "Purging DynamoDB table #{table_name}."
        table.delete

        sleep 1 while table.exists?
      end

      SkyRunner.log :info, "Creating DynamoDB table #{table_name}."

      table = dynamo_db.tables.create(table_name, 
                                     SkyRunner::dynamo_db_read_capacity, 
                                     SkyRunner::dynamo_db_write_capacity,
                                     hash_key: { id: :string },
                                     range_key: { task_id: :string })

      sleep 1 while table.status == :creating
    end

    queue = self.sqs_queue

    if !queue || params[:purge]
      queue_name = SkyRunner::sqs_queue_name

      if queue && params[:purge]
        SkyRunner.log :warn, "Purging SQS queue #{queue_name}. Waiting 65 seconds to re-create."
        queue.delete

        sleep 65
      end

      SkyRunner.log :info, "Creating SQS queue #{queue_name}."

      queue = sqs.queues.create(queue_name, 
                                visibility_timeout: SkyRunner::sqs_visibility_timeout,
                                message_retention_period: SkyRunner::sqs_message_retention_period)
    end

    true
  end

  def self.consume!(&block)
    raise "Queue #{SkyRunner::sqs_queue_name} not found. Try running 'skyrunner init'" unless sqs_queue
    raise "DynamoDB table #{SkyRunner::dynamo_db_table_name} not found. Try running 'skyrunner init'" unless dynamo_db_table && dynamo_db_table.exists?

    local_queue = Queue.new

    threads = []

    1.upto(SkyRunner::consumer_threads) do
      threads << Thread.new do
        table = SkyRunner::dynamo_db_table

        loop do
          begin
            if local_queue.empty?
              break if SkyRunner::stop_consuming?

              sleep 1
              next
            end

            klass, job_id, task_id, job_args, task_args, is_solo, message = local_queue.pop

            if klass
              begin
                unless is_solo
                  # Avoid running the same task twice, enter record and raise error if exists already.

                  SkyRunner::retry_dynamo_db do
                    table.items.put({ id: "#{job_id}-tasks", task_id: task_id }, unless_exists: ["id", "task_id"])
                  end
                end

                SkyRunner::log :info, "Run Task: #{task_args} Job: #{job_id} Message: #{message.id}"

                job = klass.new
                job.skyrunner_job_id = job_id
                job.skyrunner_job_is_solo = is_solo

                begin
                  job.consume!(job_args, task_args)
                  message.delete
                rescue Exception => e
                  message.delete rescue nil
                  block.call(e) if block_given?
                  SkyRunner::log :error, "Task Failed: #{task_args} Job: #{job_id} #{e.message} #{e.backtrace.join("\n")}"
                end
              rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException => e
                message.delete rescue nil
              end
            end
          rescue Exception => e
            puts e.message
            puts e.backtrace.join("\n")
            raise e
          end
        end
      end
    end

    1.upto((SkyRunner::consumer_threads.to_f / SQS_MAX_BATCH_SIZE).ceil + 1) do
      threads << Thread.new do
        begin
          loop do
            table = SkyRunner::dynamo_db_table
            queue = sqs_queue

            break if SkyRunner::stop_consuming?

            sleep 1 while local_queue.size >= SkyRunner::consumer_threads

            received_messages = []

            queue.receive_messages(limit: SQS_MAX_BATCH_SIZE, wait_time_seconds: 5) do |message|
              received_messages << [message, JSON.parse(message.body)]
            end

            next unless received_messages.size > 0

            job_ids = received_messages.select { |m| !m[1]["is_solo"] }.map { |m| [m[1]["job_id"], m[1]["job_id"]] }.uniq

            job_records = {}

            if job_ids.uniq.size > 0
              SkyRunner::retry_dynamo_db do
                # Read DynamoDB records into job and task lookup tables.
                table.batch_get(["id", "task_id", "failed"], job_ids.uniq, consistent_read: true) do |record|
                  job_records[record["id"]] = record
                end
              end
            end

            received_messages.each do |received_message|
              message, message_data = received_message
              job_id = message_data["job_id"]
              task_id = message_data["task_id"]
              is_solo = message_data["is_solo"]
              job_args = message_data["job_args"]
              task_args = message_data["task_args"]

              job_record = job_records[job_id]

              if is_solo || (job_record && job_record["failed"] == 0)
                begin
                  klass = Kernel.const_get(message_data["job_class"])
                  local_queue.push([klass, job_id, task_id, job_args, task_args, is_solo, message])
                rescue NameError => e
                  block.call(e) if block_given?
                  message.delete rescue nil
                  log :error, "Task Failed: No such class #{message_data["job_class"]} #{e.message}"
                end
              else
                message.delete rescue nil
              end
            end
          end
        rescue Exception => e
          puts e.message
          puts e.backtrace.join("\n")
          raise e
        end
      end
    end

    log :info, "Consumer started."

    threads.each(&:join)

    true
  end

  def self.dynamo_db_table
    table = Thread.current.thread_variable_get(:skyrunner_dyn_table)
    return table if table 

    dynamo_db.tables[SkyRunner::dynamo_db_table_name].tap do |table|
      table.load_schema if table && table.exists?
      Thread.current.thread_variable_set(:skyrunner_dyn_table, table)
    end
  end

  def self.sqs_queue
    begin
      sqs.queues.named(SkyRunner::sqs_queue_name)
    rescue AWS::SQS::Errors::NonExistentQueue => e
      return nil
    end
  end

  def self.log(type, message)
    SkyRunner::logger.send(type, "[SkyRunner] #{message}")
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

  mattr_accessor :logger
  @@logger = Log4r::Logger.new("skyrunner")

  mattr_accessor :consumer_threads
  @@consumer_threads = 10

  mattr_accessor :run_locally
  @@run_locally = false

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

  def self.retry_dynamo_db(&block)
    handler = Proc.new do |exception, num, delay|
      if exception
        SkyRunner.log :warn, "Having to retry DynamoDB requests. #{exception.message}"
      end
    end

    with_retries(handler: handler, max_tries: 100, rescue: AWS::DynamoDB::Errors::ProvisionedThroughputExceededException, base_sleep_seconds: 2, max_sleep_seconds: 60) do
      block.call
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
