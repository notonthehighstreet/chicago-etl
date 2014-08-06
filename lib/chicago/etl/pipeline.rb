module Chicago
  module ETL
    # An ETL pipeline.
    class Pipeline
      # Returns all the defined stages.
      attr_reader :stages

      # Creates a pipeline for a Schema.
      def initialize(db, schema, &block)
        @schema, @db = schema, db
        @stages = Chicago::Schema::NamedElementCollection.new
        @builder_class_factory = block || lambda {|name, options| StageBuilder }
      end

      # Defines a generic stage in the pipeline.
      def define_stage(*args, &block)
        @stages << build_stage(*args, &block)
      end

      def build_stage(*args, &block)
        options = args.last.kind_of?(Hash) ? args.pop : {}
        name = StageName.new(args)
        builder(name, options).build(name, options, &block)
      end

      private
      
      def builder(name, options)
        @builder_class_factory.call(name, options).new(@db, @schema)
      end
    end
  end
end
