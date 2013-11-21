require "aws-sdk"
require "active_support"
require "active_support/core_ext"
require "log4r"

module SkyRunner
  def self.setup
    yield self
  end

  def self.init!(params = {})
    dynamodb = AWS::DynamoDB.new

    table_name = SkyRunner.dynamodb_table
    table = dynamodb.tables[table_name]

    if !table.exists? || params[:purge]
      if table.exists? && params[:purge]
        SkyRunner.log :warn, "Purging DynamoDB table #{SkyRunner.dynamodb_table}"
        table.delete

        sleep 1 while table.exists?
      end

      SkyRunner.log :info, "Creating DynamoDB table #{SkyRunner.dynamodb_table}"

      table = dynamodb.tables.create(table_name, 
                                     SkyRunner.dynamodb_read_capacity, 
                                     SkyRunner.dynamodb_write_capacity,
                                     hash_key: { id: :string })

      sleep 1 while table.status == :creating
    end
  end

  def self.log(type, message)
    SkyRunner.logger.send(type, "[SkyRunner] #{message}")
  end

  mattr_accessor :dynamodb_table
  @@dynamodb_table = "skyrunner_jobs"

  mattr_accessor :dynamodb_read_capacity
  @@dynamodb_read_capacity = 10

  mattr_accessor :dynamodb_write_capacity
  @@dynamodb_write_capacity = 10

  mattr_accessor :sqs_queue
  @@sqs_queue = "skyrunner_tasks"

  mattr_accessor :job_namespace
  @@job_namespace = "default"

  mattr_accessor :logger
  @@logger = Log4r::Logger.new("skyrunner")
end

require "skyrunner/job"
