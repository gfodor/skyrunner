# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'skyrunner/version'

Gem::Specification.new do |spec|
  spec.name          = "skyrunner"
  spec.version       = Skyrunner::VERSION
  spec.authors       = ["Greg Fodor"]
  spec.email         = ["gfodor@gmail.com"]
  spec.description   = %q{SkyRunner runs logical jobs that are broken up into tasks via Amazon SQS and DynamoDB.}
  spec.summary       = %q{SkyRunner runs logical jobs that are broken up into tasks via Amazon SQS and DynamoDB.}
  spec.homepage      = "http://github.com/gfodor/skyrunner"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"

  gem.add_runtime_dependency "aws-sdk"
  gem.add_runtime_dependency "activesupport", "~> 4.0"
  gem.add_runtime_dependency "log4r"
  gem.add_runtime_dependency "trollop"
end
