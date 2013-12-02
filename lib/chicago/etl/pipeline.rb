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
        @stages << builder(name, options).build(name, &block)
      end

      def build_stage(name, schema_table, &block)
        SchemaTableStageBuilder.new(@db, schema_table).build(name, &block)
      end

      private

      def builder(name, options)
        if name =~ [:load, :dimensions]
          dimension_load_stage_builder(name, options)
        elsif name =~ [:load, :facts]
          fact_load_stage_builder(name, options)
        else
          StageBuilder.new(@db)
        end
      end

      def dimension_load_stage_builder(name, options)
        dimension_name = options[:dimension] || name.name
        SchemaTableStageBuilder.new(@db, @schema.dimension(dimension_name))
      end

      def fact_load_stage_builder(name, options)
        fact_name = options[:fact] || name.name
        SchemaTableStageBuilder.new(@db, @schema.fact(fact_name))
      end
    end
  end
end
