module Chicago
  module ETL
    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class SchemaTableStageBuilder
      # @api private
      def initialize(db, schema_table)
        @db, @schema_table = db, schema_table
      end

      # @api private
      def build(name, &block)
        instance_eval &block
        unless defined? @sinks_and_transformations
          pipeline do
          end
        end

        @filter_strategy ||= lambda {|dataset, etl_batch| 
          dataset.filter_to_etl_batch(etl_batch)
        }
        
        DatasetBatchStage.new(name,
                              :source => @dataset, 
                              :transformations => @sinks_and_transformations[:transformations],
                              :sinks => @sinks_and_transformations[:sinks],
                              :filter_strategy => @filter_strategy,
                              :truncate_pre_load => @truncate_pre_load)
      end

      protected
      
      # Specifies that the sinks should be truncated before loading
      # data.
      def truncate_pre_load
        @truncate_pre_load = true
      end

      # Specifies that the dataset should never be filtered to the ETL
      # batch - i.e. it should behave as if reextract was always true
      def full_reload
        @filter_strategy = lambda {|dataset, etl_batch| dataset }
      end

      # Define elements of the pipeline. See LoadPipelineStageBuilder
      # for details.
      # TODO: rename pipeline => transforms below this method
      def pipeline(&block)
        @sinks_and_transformations = SchemaSinksAndTransformationsBuilder.new(@db, @schema_table).
          build(&block)
      end

      # Defines the dataset, see DatasetBuilder .
      #
      # The block must return a Sequel::Dataset.
      # TODO: rename dataset => source below this method, make generic
      def source(&block)
        @dataset = DatasetBuilder.new(@db).build(&block)
      end
      alias :dataset :source

      # Define a custom filter strategy for filtering to an ETL batch.
      def filter_strategy(&block)
        @filter_strategy = block
      end
    end
  end
end
