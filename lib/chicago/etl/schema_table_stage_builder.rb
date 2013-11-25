require 'chicago/etl/stage_builder'

module Chicago
  module ETL
    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class SchemaTableStageBuilder < StageBuilder
      # @api private
      def initialize(db, schema_table)
        super(db)
        @wrapped_builder = SchemaSinksAndTransformationsBuilder.
          new(@db, schema_table)
      end

      protected
      
      # Define elements of the pipeline. See LoadPipelineStageBuilder
      # for details.
      #
      # @deprecated
      def pipeline(&block)
        sinks_and_transformations = @wrapped_builder.build(&block)
        @sinks = sinks_and_transformations[:sinks]
        @transformations = sinks_and_transformations[:transformations] || []
      end

      # @api private
      def set_default_stage_values
        unless defined? @sinks
          pipeline do
          end
        end

        @pre_execution_strategies << lambda {|stage, etl_batch, reextract|
          stage.sink(:error).truncate if reextract && stage.sink(:error)
          stage.sink(:default).
            set_constant_values(:_inserted_at => Time.now)
        }

        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
      end
    end
  end
end
