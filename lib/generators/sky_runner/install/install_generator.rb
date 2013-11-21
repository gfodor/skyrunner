require 'rails/generators'

module SkyRunner
  module Generators
    class InstallGenerator < Rails::Generators::Base
      desc "Generator for SkyRunner configuration files."

      # Commandline options can be defined here using Thor-like options:
      #class_option :my_opt, :type => :boolean, :default => false, :desc => "My Option"

      # I can later access that option using:
      # options[:my_opt]


      def self.source_root
        @source_root ||= File.join(File.dirname(__FILE__), 'templates')
      end

      # Generator Code. Remember this is just suped-up Thor so methods are executed in order

      def create_initializer_file
        copy_file "skyrunner.rb", "config/initializers/skyrunner.rb"
      end

      def create_rake_tasks
        copy_file "skyrunner.rake", "lib/tasks/skyrunner.rake"
      end
    end
  end
end

