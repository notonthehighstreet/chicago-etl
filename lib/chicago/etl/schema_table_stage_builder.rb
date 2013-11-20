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
        @schema_table = schema_table
      end

      # @api private
      def build(name, &block)
        instance_eval &block

        unless defined? @sinks
          pipeline do
          end
        end

        @pre_execution_strategy = determine_pre_execution_strategy
        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
        
        Stage.new(name,
                  :source => @dataset, 
                  :transformations => @transformations,
                  :sinks => @sinks,
                  :filter_strategy => @filter_strategy,
                  :pre_execution_strategy => @pre_execution_strategy)
      end

      protected
      
      # Define elements of the pipeline. See LoadPipelineStageBuilder
      # for details.
      #
      # @deprecated
      def pipeline(&block)
        sinks_and_transformations = SchemaSinksAndTransformationsBuilder.
          new(@db, @schema_table).build(&block)
        @sinks = sinks_and_transformations[:sinks]
        @transformations = sinks_and_transformations[:transformations] || []
      end

      def determine_pre_execution_strategy
        if @truncate_pre_load
          lambda {|stage, etl_batch, reextract|
            stage.sinks.each {|sink| sink.truncate }
            stage.sink(:default).
              set_constant_values(:_inserted_at => Time.now)
          }
        else
          lambda {|stage, etl_batch, reextract|
            stage.sink(:error).truncate if reextract && stage.sink(:error)
            stage.sink(:default).
              set_constant_values(:_inserted_at => Time.now)
          }
        end
      end

      alias :dataset :source
    end
  end
end
