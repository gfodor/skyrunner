namespace :skyrunner do
  desc "Creates DynamoDB table and SQS queue for SkyRunner."
  task init: :environment do
    SkyRunner.init!
  end

  desc "Starts consuming SkyRunner tasks."
  task consume: :environment do
    trap("INT") do
      SkyRunner.stop_consuming!(true)
    end

    SkyRunner.consume! do |exception|
      if exception
        # TODO customize this with what you want to do if there is an exception.
        # (For example, send to Airbrake.)
      end
    end
  end

  desc "Purges and re-creates DynamoDB table and SQS queue for SkyRunner. (Warning: destructive!)"
  task purge: :environment do
    SkyRunner.purge!
  end
end
