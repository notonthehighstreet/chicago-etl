require 'chicago/etl/stage_builder'

module Chicago
  module ETL
    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class SchemaTableStageBuilder < StageBuilder
      def build(name, options, &block)
        @wrapped_builder = SchemaSinksAndTransformationsBuilder.
          new(@db, schema_table(name, options))

        super
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

        @pre_execution_strategies << lambda {|stage, etl_batch|
          if etl_batch.reextracting? && stage.sink(:error)
            stage.sink(:error).truncate
          end

          stage.sink(:default).
            set_constant_values(:_inserted_at => Time.now.utc)
        }

        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
      end
    end

    class LoadDimensionStageBuilder < SchemaTableStageBuilder
      def schema_table(name, options)
        dimension_name = options[:dimension] || name.name
        @schema.dimension(dimension_name)
      end
    end

    class LoadFactStageBuilder < SchemaTableStageBuilder
      def schema_table(name, options)
        fact_name = options[:dimension] || name.name
        @schema.fact(fact_name)
      end
    end
  end
end
