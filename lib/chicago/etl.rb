if RUBY_VERSION.split(".")[1] < "9"
  require 'fastercsv'
  CSV = FasterCSV
else
  require 'csv'
end

require 'sequel'
require 'chicago/etl/errors'
require 'chicago/etl/transformation'
require 'chicago/etl/filter'
require 'chicago/etl/transformation_chain'
require 'chicago/etl/pipeline_endpoint'
require 'chicago/etl/array_source'
require 'chicago/etl/dataset_source'
require 'chicago/etl/sink'
require 'chicago/etl/array_sink'
require 'chicago/etl/null_sink'
require 'chicago/etl/mysql'

require 'chicago/etl/core_extensions'
require 'chicago/etl/stage_name'
require 'chicago/etl/counter'
require 'chicago/etl/key_builder'
require 'chicago/etl/schema_table_sink_factory'
require 'chicago/etl/schema_table_stage_builder'
require 'chicago/etl/transformations'
require 'chicago/etl/load_dataset_builder'
require 'chicago/etl/dataset_builder'
require 'chicago/etl/basic_stage'
require 'chicago/etl/stage'
require 'chicago/etl/direct_update_stage'
require 'chicago/etl/stage_builder'
require 'chicago/etl/schema_sinks_and_transformations_builder'
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
require 'chicago/etl/transformations/deduplicate_rows'
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
    def self.execute(stage, etl_batch, logger)
      etl_batch.perform_task(:load, stage.name) do
        if stage.executable?
          logger.debug "Starting executing stage: #{stage.name}"
          stage.execute etl_batch
          logger.info "Finished executing stage: #{stage.name}"
        else
          logger.info "Skipping stage #{stage.name}"
        end
      end
    end
  end

  # Deprecated, allows clients to transition when they like.
  Flow = ETL
end
