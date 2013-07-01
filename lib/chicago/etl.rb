require 'sequel'
require 'chicago/flow'
require 'chicago/flow/mysql'

require 'chicago/etl/core_extensions'
require 'chicago/etl/counter'
require 'chicago/etl/key_builder'
require 'chicago/etl/schema_table_sink_factory'
require 'chicago/etl/transformations'
require 'chicago/etl/load_dataset_builder'
require 'chicago/etl/dataset_batch_stage'

# Sequel Extensions
require 'chicago/etl/sequel/filter_to_etl_batch'
require 'chicago/etl/sequel/dependant_tables'

# Screens
require 'chicago/etl/screens/column_screen'
require 'chicago/etl/screens/missing_value'
require 'chicago/etl/screens/invalid_element'
require 'chicago/etl/screens/out_of_bounds'

# Transformations
require 'chicago/etl/transformations/uk_post_code'
require 'chicago/etl/transformations/uk_post_code_field'

module Chicago
  module ETL
    autoload :TableBuilder,   'chicago/etl/table_builder.rb'
    autoload :Batch,          'chicago/etl/batch.rb'
    autoload :TaskInvocation, 'chicago/etl/task_invocation.rb'
  end
end
