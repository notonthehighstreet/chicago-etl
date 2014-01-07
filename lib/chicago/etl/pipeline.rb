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
        @stages << builder(name, options).build(name, options, &block)
      end

      private

      def builder(name, options)
        builder_class(name, options).new(@db, @schema)
      end

      # TODO: factor out - we shouldn't be hardcoding what to do with
      # specific names - too project specific.
      def builder_class(name, options)
        if name =~ [:load, :dimensions]
          LoadDimensionStageBuilder
        elsif name =~ [:load, :facts]
          LoadFactStageBuilder
        else
          StageBuilder
        end
      end
    end
  end
end
