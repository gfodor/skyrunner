SkyRunner
=========

SkyRunner is a simple job execution framework that lets you run jobs which are composed of small tasks. SkyRunner uses AWS SQS and DynamoDB for queueing and job coordination, so you don't need any servers other than your task consumers to run jobs. 

The key feature provided is that once a job's last task completes, a completion method can be called so you can perform a post-processing step. For example, you may run a job that has 100 tasks that process image frames in parallel, and then when these tasks are completed you compose the frames into a video and upload it to S3. Of course, you can also execute another job in this method, so you can easily chain jobs together.

Add to your Gemfile:

```
gem "skyrunner"
```

To set up with Rails:

```
bundle exec rails g sky_runner:install
```

Customize `config/initializers/skyrunner.rb`. Update `lib/tasks/skyrunner.rake` to do what you want if there are exceptions during a task.

Put your AWS keys in the usual environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. If you are running on EC2, you don't need to do this and SkyRunner will run within the machine's IAM role.

Be sure the IAM permissions are granted to create DynamoDB tables and SQS queues, and to perform all read and write operations to the table and queue you've configured for SkyRunner.

To initialize DynamoDB & SQS:

``
bundle exec rake skyrunner:init
``

To start a consumer:

``
bundle exec rake skyrunner:consume
``

To gracefully shut down a consumer, send it SIGINT (or hit Ctrl-C if you have a console.) It will finish up processing the tasks it has de-queued before terminating.

See `jobs/example_job.rb` for an example job. To run a job, just call `execute!` on the job, passing any named job arguments you want. The job class should implement the method `run`. This method will get passed the job arguments. For each task you want consumers to run, `run` should yield an array of two elements, the first being the name of the method on the job class to run for the task, and the second a Hash of method arguments. Note that the consumer is (by default) multi-threaded, so please be sure your task methods are thread-safe.

You can specify `on_complete` and `on_failure` method(s) to call when the tasks are all completed, or if any of them fail, respectively. These methods will also be passed the original job arguments. Importantly, the completion method is guaranteed to be called once and only once, when the final task has been completed.

If any of your tasks fail in a job, consumers will stop consuming tasks for that job and deplete any queued tasks on SQS. SkyRunner does not currently retry failed tasks, for now you'll need to implement your own retry logic in your task method instead.

