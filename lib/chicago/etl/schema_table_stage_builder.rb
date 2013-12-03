require 'chicago/etl/stage_builder'

module Chicago
  module ETL
    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class SchemaTableStageBuilder < StageBuilder
      # @api private
      KeyMapping = Struct.new(:table, :field)

      protected
      
      # This should be removed at some point - should not be
      # necessary. Must be called before sinks & transformations are called
      def load_separately(*columns)
        @load_separately = columns
      end

      def key_mapping(table, field)
        @key_mappings << KeyMapping.new(table, field)
      end

      # @api private
      def set_default_stage_values
        super

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

        @load_separately ||= []
        @key_mappings = []
      end

      def transformation_builder
        SchemaTableTransformationBuilder.
          new(@db, @schema_table, @load_separately, @key_mappings)
      end

      def sink_builder
        SchemaTableSinkBuilder.new(@db, @schema_table, @key_mappings)
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
