module Chicago
  module ETL
    # An ETL pipeline.
    class Pipeline
      # Returns all the defined stages.
      attr_reader :stages

      # Creates a pipeline for a Schema.
      def initialize(db, schema)
        @schema, @db = schema, db
        @stages = Chicago::Schema::NamedElementCollection.new
      end

      # Defines a generic stage in the pipeline.
      def define_stage(*args, &block)
        options = args.last.kind_of?(Hash) ? args.pop : {}

        name = StageName.new(args)
        
        if name =~ [:load, :dimensions]
          @stages << build_dimension_load_stage(name, options, &block)
        elsif name =~ [:load, :facts]
          @stages << build_fact_load_stage(name, options, &block)
        else
          @stages << StageBuilder.new(@db).build(name, &block)
        end
      end

      def build_stage(name, schema_table, &block)
        SchemaTableStageBuilder.new(@db, schema_table).build(name, &block)
      end

      private

      def build_dimension_load_stage(name, options, &block)
        dimension_name = options[:dimension] || name.name
        build_stage(name, @schema.dimension(dimension_name), &block)
      end

      def build_fact_load_stage(name, options, &block)
        fact_name = options[:fact] || name.name
        build_stage(name, @schema.fact(fact_name), &block)
      end
    end
  end
end
