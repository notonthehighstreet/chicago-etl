module Chicago
  module ETL
    class StageBuilder
      def initialize(db)
        @db = db
      end

      def build(name, &block)
        @sinks = {}
        @transformations = []

        instance_eval &block
        
        Stage.new(name,
                  :source => @dataset, 
                  :sinks => @sinks, 
                  :transformations => @transformations, 
                  :filter_strategy => @filter_strategy)
      end

      protected

      def source(&block)
        @dataset = DatasetBuilder.new(@db).build(&block)
      end
      
      def transformations(&block)
        @transformations = TransformationBuilder.new.build(&block)
      end

      def sinks(&block)
        @sinks = SinkBuilder.new.build(&block)
      end

      # TODO: think of potentially better ways of dealig with this
      # problem.
      def filter_strategy(&block)
        @filter_strategy = block
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
