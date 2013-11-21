SkyRunner.setup do |config|
  # Customize these to change the name of the table & queue used for job and task management.
  config.dynamo_db_table_name = "skyrunner_jobs_#{Rails.env}"
  config.sqs_queue_name = "skyrunner_tasks_#{Rails.env}"

  # Customzie this to change the namespace of jobs enqueued and consumed by this application.
  config.job_namespace = Rails.application.class.parent_name.underscore

  # Set the number of tasks for a consumer to pull and run from SQS at a time. (Max 10, default 10)
  #
  # config.consumer_batch_size = 10

  # Set the visibility timeout of queue items. If the consumer batch size (above) is set to 10,
  # this should provide sufficient time for a consumer to process 10 tasks, for example. (default 90)
  #
  # config.visibility_timeout = 90
end
