module SkyRunner::Job
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods
    def on_completed(methods)
      add_job_event_methods(methods, :completed)
    end

    def on_failed(methods)
      add_job_event_methods(methods, :failed)
    end

    private 

    def job_event_methods
      @_job_event_methods ||= {}
    end

    def add_job_event_methods(methods, type)
      methods = Array(methods)
      job_event_methods[type] ||= []
      job_event_methods[type].concat(methods.map(&:to_sym))
    end

    def fire_job_event_methods(type)
      Array(job_event_methods[type]).each do |method|
        self.send(method)
      end
    end
  end
end
