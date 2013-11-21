require "skyrunner"

module ExampleJobModule
  class ExampleJob
    include SkyRunner::Job

    on_completed :print_completed
    on_failed :print_failed

    def run(number_of_tasks: nil)
      1.upto(number_of_tasks).each do |n|
        yield :print_number, task_number: n
      end
    end

    def print_number(task_number: nil)
      puts "Ran rask #{task_number}"
    end

    def print_completed(number_of_tasks: nil)
      puts "Completed with #{number_of_tasks}"
    end

    def print_failed(number_of_tasks: nil)
      puts "Failed with #{number_of_tasks}"
    end
  end
end
