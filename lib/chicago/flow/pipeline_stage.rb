module Chicago
  module Flow
    # Co-ordinates iterating over rows provided by a source, passing
    # them through a transformation chain before writing them to
    # sink(s).
    #
    # @api public
    class PipelineStage
      attr_reader :transformation_chain, :transformations
      
      def initialize(options={})
        @sinks  = options[:sinks] || {}
        @transformations = options[:transformations] || []
        @transformation_chain = TransformationChain.new(*@transformations)
      end

      # Returns the named sink, if it exists
      def sink(name)
        @sinks[name.to_sym]
      end

      
      def sinks
        @sinks
      end

      def register_sink(name, sink)
        @sinks[name.to_sym] = sink
        self
      end
            
      def execute(source)
        sinks.values.each(&:open)
        pipe_rows_to_sinks_from(source)
        sinks.values.each(&:close)
      end
      
      private
      
      def pipe_rows_to_sinks_from(source)
        source.each do |row|
          transformation_chain.process(row).each {|row| process_row(row) }
        end
        transformation_chain.flush.each {|row| process_row(row) }
      end

      def process_row(row)
        stream = row.delete(:_stream) || :default
        @sinks[stream] << row
      end
    end
  end
end
