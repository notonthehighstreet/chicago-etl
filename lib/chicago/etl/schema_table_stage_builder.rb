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

        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
        
        DatasetBatchStage.new(name,
                              :source => @dataset, 
                              :transformations => @transformations,
                              :sinks => @sinks,
                              :filter_strategy => @filter_strategy,
                              :truncate_pre_load => @truncate_pre_load)
      end

      protected
      
      # Specifies that the dataset should never be filtered to the ETL
      # batch - i.e. it should behave as if reextract was always true
      def full_reload
        @filter_strategy = lambda {|dataset, etl_batch| dataset }
      end

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

      alias :dataset :source
    end
  end
end
