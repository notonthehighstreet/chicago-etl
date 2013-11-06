module Chicago
  module ETL
    # An ETL pipeline.
    class Pipeline
      # Returns all defined dimension load tasks
      attr_reader :load_dimensions

      # Returns all defined fact load tasks
      attr_reader :load_facts

      # Returns all the defined generic stages.
      attr_reader :stages

      # Creates a pipeline for a Schema.
      def initialize(db, schema)
        @schema, @db = schema, db
        @load_dimensions = Chicago::Schema::NamedElementCollection.new
        @load_facts = Chicago::Schema::NamedElementCollection.new
        @stages = Chicago::Schema::NamedElementCollection.new
      end

      # Defines a generic stage in the pipeline.
      def define_stage(name, &block)
        @stages << build_schemaless_stage(name, &block)
      end

      def build_schemaless_stage(name, &block)
        StageBuilder.new(@db).build(name, &block)
      end

      # Defines a dimension load stage
      def define_dimension_load(name, options={}, &block)
        dimension_name = options[:dimension] || name
        @load_dimensions << build_stage(name, 
                                        @schema.dimension(dimension_name),
                                        &block)
      end

      # Defines a fact load stage
      def define_fact_load(name, options={}, &block)
        fact_name = options[:fact] || name
        @load_facts << build_stage(name, @schema.fact(fact_name), &block)
      end

      # Builds a stage, but does not define it.
      def build_stage(name, schema_table, &block)
        DatasetBatchStageBuilder.new(@db, schema_table).build(name, &block)
      end
    end

    # Provides DSL methods for building a DataSetBatchStage.
    #
    # Clients shouldn't need to instantiate this directly, but instead
    # call the protected methods in the context of defining a Pipeline
    class DatasetBatchStageBuilder
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
