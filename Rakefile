# encoding: utf-8

require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  gem.name = "chicago-flow"
  gem.homepage = "http://github.com/notonthehighstreet/chicago-flow"
  gem.license = "MIT"
  gem.summary = "Dataflow-style processing for hash-like rows"
  gem.description = "Dataflow-style processing for hash-like rows"
  gem.email = "roland.swingler@gmail.com"
  gem.authors = ["Roland Swingler"]
  # dependencies defined in Gemfile
end
Jeweler::RubygemsDotOrgTasks.new

require 'rspec/core'
require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end

task :default => :spec
