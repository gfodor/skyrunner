require "skyrunner"

# Example job.
#
# To run:
#
# ExampleJobModule::ExampleJob.new.execute!(number_of_tasks: 7)
#
module ExampleJobModule
  class ExampleJob
    include SkyRunner::Job

    on_completed :print_completed
    on_failed :print_failed

    def run(number_of_tasks: nil)
      # Yield the method and arguments you want to call for each task. 
      # 
      # The number of times this method yields is the number of tasks that consumers
      # will run.
      1.upto(number_of_tasks).each do |n|
        yield :print_number, task_number: n
      end
    end

    def print_number(task_number: nil)
      sleep 0.2
      puts "Ran task #{task_number}"
    end

    def print_completed(number_of_tasks: nil)
      puts "Completed with #{number_of_tasks} tasks requested."
    end

    def print_failed(number_of_tasks: nil)
      puts "Example Job Failed."
    end
  end
end
