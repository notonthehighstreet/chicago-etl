require 'chicago/etl/stage_builder'

module Chicago
  module ETL
    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class SchemaTableStageBuilder < StageBuilder
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

      def load_separately(*columns)
        @load_separately = columns
      end

      # @api private
      def set_default_stage_values
        unless defined? @sinks
          pipeline do
          end
        end

        @sinks[:default].set_columns(load_columns(@load_separately))

        @pre_execution_strategies << lambda {|stage, etl_batch|
          if etl_batch.reextracting? && stage.sink(:error)
            stage.sink(:error).truncate
          end

          stage.sink(:default).
            set_constant_values(:_inserted_at => Time.now)
        }

        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
      end

      
      def parse_options(name, options)
        if name =~ [:load, :dimensions]
          dimension_name = options[:dimension] || name.name
          @schema_table = schema.dimension(dimension_name)
        else
          fact_name = options[:fact] || name.name
          @schema_table = schema.fact(fact_name)
        end

        @wrapped_builder = SchemaSinksAndTransformationsBuilder.
          new(db, @schema_table)
      end

      def load_columns(exclude=nil)
        exclude = [exclude].compact.flatten
        [:id] + @schema_table.columns.
          reject {|c| exclude.include?(c.name) }.
          map {|c| c.database_name }
      end
    end
  end
end
