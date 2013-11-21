require 'rails/generators'

module SkyRunner
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
      copy_file "config/initializers/skyrunner.rb", "config/initializers/skyrunner.rb"
    end
  end
end

