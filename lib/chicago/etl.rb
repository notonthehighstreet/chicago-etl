if RUBY_VERSION.split(".")[1] < "9"
  require 'fastercsv'
  CSV = FasterCSV
else
  require 'csv'
end

require 'sequel'
require 'chicago/flow/errors'
require 'chicago/flow/transformation'
require 'chicago/flow/filter'
require 'chicago/flow/transformation_chain'
require 'chicago/flow/pipeline_stage'
require 'chicago/flow/pipeline_endpoint'
require 'chicago/flow/array_source'
require 'chicago/flow/dataset_source'
require 'chicago/flow/sink'
require 'chicago/flow/array_sink'
require 'chicago/flow/null_sink'
require 'chicago/flow/mysql'

require 'chicago/etl/core_extensions'
require 'chicago/etl/counter'
require 'chicago/etl/key_builder'
require 'chicago/etl/schema_table_sink_factory'
require 'chicago/etl/transformations'
require 'chicago/etl/load_dataset_builder'
require 'chicago/etl/dataset_batch_stage'
require 'chicago/etl/load_pipeline_stage_builder'
require 'chicago/etl/pipeline'

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
  # Contains classes related to ETL processing.
  module ETL
    autoload :TableBuilder,   'chicago/etl/table_builder.rb'
    autoload :Batch,          'chicago/etl/batch.rb'
    autoload :TaskInvocation, 'chicago/etl/task_invocation.rb'

    # Executes a pipeline stage in the context of an ETL Batch.
    #
    # Tasks execution status is stored in a database etl task
    # invocations table - this ensures tasks aren't run more than once
    # within a batch.
    def self.execute(stage, etl_batch, reextract, logger)
      etl_batch.perform_task(:load, stage.name) do
        logger.debug "Starting loading #{stage.name}"
        stage.execute(etl_batch, reextract)
        logger.debug "Finished loading #{stage.name}"
      end
    end
  end
end
