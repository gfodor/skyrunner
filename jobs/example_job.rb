require "skyrunner"

class ExampleJob
  include SkyRunner::Job

  on_completed :print_completed
  on_failed :print_failed

  def run_example_job(number_of_tasks: 10)
    1.upto(number_of_tasks).each do |n|
      yield :print_number, task_number: n
    end
  end

  def print_number(task_number: nil)
    puts "Ran rask #{task_number}"
  end

  def print_completed
    puts "Complete"
  end

  def print_failed
    puts "Failed"
  end
end
