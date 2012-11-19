require 'sequel'

require 'chicago/etl/key_builder'
require 'chicago/etl/sink'
require 'chicago/etl/mysql_load_file_value_transformer'
require 'chicago/etl/buffering_insert_writer'
require 'chicago/etl/mysql_dumpfile'

require 'chicago/etl/load_dataset_builder'

# Sequel Extensions
require 'chicago/etl/sequel/filter_to_etl_batch'
require 'chicago/etl/sequel/load_data_infile'
require 'chicago/etl/sequel/dependant_tables'

# Screens
require 'chicago/etl/screens/column_screen'
require 'chicago/etl/screens/composite_screen'
require 'chicago/etl/screens/missing_value'
require 'chicago/etl/screens/invalid_element'
require 'chicago/etl/screens/out_of_bounds'

# Transformations
require 'chicago/etl/transformations/add_insert_timestamp'
require 'chicago/etl/transformations/uk_post_code'
require 'chicago/etl/transformations/uk_post_code_field'

module Chicago
  module ETL
    autoload :TableBuilder,   'chicago/etl/table_builder.rb'
    autoload :Batch,          'chicago/etl/batch.rb'
    autoload :TaskInvocation, 'chicago/etl/task_invocation.rb'
  end
end
