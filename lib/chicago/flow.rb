if RUBY_VERSION.split(".")[1] < "9"
  require 'fastercsv'
  CSV = FasterCSV
else
  require 'csv'
end

require 'chicago/flow/transformation'
require 'chicago/flow/filter'
require 'chicago/flow/transformation_chain'
require 'chicago/flow/pipeline_stage'
require 'chicago/flow/pipeline_endpoint'
require 'chicago/flow/array_source'
require 'chicago/flow/dataset_source'
require 'chicago/flow/sink'
require 'chicago/flow/array_sink'
