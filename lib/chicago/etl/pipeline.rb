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
        @stages << build_stage(*args, &block)
      end

      def build_stage(*args, &block)
        options = args.last.kind_of?(Hash) ? args.pop : {}
        name = StageName.new(args)
        builder(name).build(name, options, &block)
      end

      private

      def builder(name)
        if name =~ [:load, :dimensions] || name =~ [:load, :facts]
          SchemaTableStageBuilder.new(@db, @schema)
        else
          StageBuilder.new(@db, @schema)
        end
      end
    end
  end
end
