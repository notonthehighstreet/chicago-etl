module Chicago
  module ETL
    class StageBuilder
      attr_reader :sink_factory

      def initialize(db)
        @db = db
      end

      def build(name, &block)
        @sinks = {}
        @transformations = []

        instance_eval &block
        
        Stage.new(name, :source => @dataset, :sinks => @sinks, :transformations => @transformations)
      end

      def source(&block)
        @dataset = DatasetBuilder.new(@db).build(&block)
      end
      
      def transformations(klass=TransformationBuilder, &block)
        @transformations = klass.new.build(&block)
      end

      def sinks(options={}, &block)
        @sinks = SinkBuilder.new.build(&block)
      end

      class TransformationBuilder
        def build(&block)
          @transformations = []
          instance_eval(&block)
          @transformations
        end
        
        def add(transformation)
          @transformations << transformation
        end
      end

      class SinkBuilder
        def build(&block)
          @sinks = {}
          instance_eval(&block)
          @sinks
        end

        protected

        def add(sink, options={})
          stream = options[:stream] || :default
          @sinks[stream] = sink
        end
      end
    end
  end
end
