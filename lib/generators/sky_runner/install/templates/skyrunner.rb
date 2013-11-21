SkyRunner.setup do |config|
  config.logger = Rails.logger

  # Customize these to change the name of the table & queue used for job and task management.
  config.dynamo_db_table_name = "skyrunner_jobs_#{Rails.env}"
  config.sqs_queue_name = "skyrunner_tasks_#{Rails.env}"

  # Set the visibility timeout of queue items. 
  #
  # This should provide sufficient time for a consumer to process 10 tasks, since 10 items 
  # are dequeued at a time from SQS.
  #
  # config.visibility_timeout = 90

  # Set the number of concurrent consumer threads when running the consumer.
  # (If greater than one, you obviously need to make sure your tasks are thread-safe.)
  #
  # config.consumer_threads = 10
end
