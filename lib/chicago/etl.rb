require 'chicago/etl/key_builder'
require 'chicago/etl/mysql_dumpfile_writer'
require 'chicago/etl/batched_dataset_filter'

module Chicago
  module ETL
    autoload :TableBuilder,   'chicago/etl/table_builder.rb'
    autoload :Batch,          'chicago/etl/batch.rb'
    autoload :TaskInvocation, 'chicago/etl/task_invocation.rb'
  end
end
