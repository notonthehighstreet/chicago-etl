module Chicago
  module ETL
    class Pipeline
      attr_reader :load_dimensions, :load_facts

      def initialize(db, schema)
        @schema, @db = schema, db
        @load_dimensions = Chicago::Schema::NamedElementCollection.new
        @load_facts = Chicago::Schema::NamedElementCollection.new
      end

      def define_dimension_load(name, options={}, &block)
        dimension_name = options[:dimension] || name
        @load_dimensions << build_stage(name, 
                                        @schema.dimension(dimension_name))
      end

      def define_fact_load(name, options={}, &block)
        fact_name = options[:fact] || name
        @load_facts << build_stage(name, @schema.fact(fact_name))
      end

      def build_stage(name, schema_table)
        DatasetBatchStageBuilder.new(@db, schema_table).build(name, &block)
      end
    end

    class DatasetBatchStageBuilder
      def initialize(db, schema_table)
        @db, @schema_table = db, schema_table
      end

      def build(name, &block)
        instance_eval &block
        unless defined? @pipeline_stage
          pipeline do
          end
        end
        DatasetBatchStage.new(name, @dataset, @pipeline_stage,
                              :filter_strategy => @filter_strategy,
                              :truncate_pre_load => @truncate_pre_load)
      end

      protected
      
      def truncate_pre_load
        @truncate_pre_load = true
      end

      def full_reload
        @filter_strategy = lambda {|dataset, etl_batch| dataset }
      end

      def pipeline(&block)
        @pipeline_stage = LoadPipelineStageBuilder.new(@db, @schema_table).
          build(&block)
      end

      def dataset(&block)
        @dataset = DatasetBuilder.new(@db).build(&block)
      end

      def filter_strategy(&block)
        @filter_strategy = block
      end
    end

    class DatasetBuilder
      attr_reader :db

      def initialize(db)
        @db = db
      end

      def build(&block)
        instance_eval(&block)
      end

      protected
      
      def key_field(field, name)
        :if[{field => nil}, 1, field].as(name)
      end
      
      def dimension_key(name)
        key_field("keys_dimension_#{name}__dimension_id".to_sym,
                  "#{name}_dimension_id".to_sym)
      end

      def date_dimension_column(dimension)
        :if.sql_function({:id.qualify(dimension) => nil},
                         1, 
                         :id.qualify(dimension)).
          as("#{dimension}_dimension_id".to_sym)
      end

      def round(stmt, direction = :none)
        case direction
        when :none
          :round.sql_function(stmt, 2)
        when :up
          :ceil.sql_function(stmt * 100) / 100
        when :down
          :floor.sql_function(stmt * 100) / 100
        end
      end
    end
  end
end
