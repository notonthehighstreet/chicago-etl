 source "http://rubygems.org"

gem "chicagowarehouse", "~> 0.4", ">= 0.4.6"
gem "fastercsv", :platform => :ruby_18
gem "sequel_load_data_infile", :git => "git://github.com/notonthehighstreet/sequel_load_data_infile.git", :ref => "d7449efe5b775332279f91024a028f7fa3de4713"
gem "sequel_fast_columns", :require => "sequel/fast_columns"

# Add dependencies to develop your gem here.
# Include everything needed to run rake, tests, features, etc.
group :development do
  gem "rspec", "~> 2"
  gem "timecop"
  gem "yard"
  gem "flog"
  gem "simplecov", :platforms => :mri_19, :require => false
  gem "mysql2"
  gem "bundler", "~> 1"
  gem "jeweler"
end
