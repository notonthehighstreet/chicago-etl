module Chicago
  module ETL
    class StageBuilder
      attr_reader :db, :schema
      
      def initialize(db, schema)
        @db = db
        @schema = schema
      end

      def build(name, options={}, &block)
        parse_options(name, options)

        @pre_execution_strategies = []
        @executable = true

        instance_eval &block
        set_default_stage_values

        Stage.new(name,
                  :source => @dataset, 
                  :sinks => @sinks, 
                  :transformations => @transformations, 
                  :filter_strategy => @filter_strategy,
                  :pre_execution_strategies => @pre_execution_strategies,
                  :executable => @executable)
      end

      protected

      # Specifies that the sinks should be truncated before loading
      # data.
      def truncate_pre_load
        @pre_execution_strategies << lambda {|stage, etl_batch|
          stage.sinks.each {|sink| sink.truncate }
        }
      end

      # Specifies that the dataset should never be filtered to the ETL
      # batch - i.e. it should behave as if the batch is reextracting
      def full_reload
        @filter_strategy = lambda {|dataset, etl_batch| dataset }
      end

      # Mark this stage as executable or non-executable.
      def executable(value=true)
        @executable = value
      end

      def source(&block)
        @dataset = DatasetBuilder.new(@db).build(&block)
      end
      
      def transformations(&block)
        @transformations = transformation_builder.build(&block)
      end

      def sinks(&block)
        @sinks = sink_builder.build(&block)
      end

      # TODO: think of potentially better ways of dealing with this
      # problem.
      def filter_strategy(&block)
        @filter_strategy = block
      end

      # @api private
      def set_default_stage_values
        @sinks ||= sinks {}
        @transformations ||= transformations {}
      end

      # @api private
      def parse_options(name, options)
      end

      # @api private
      def transformation_builder
        TransformationBuilder.new
      end

      # @api private
      def sink_builder
        SinkBuilder.new
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
