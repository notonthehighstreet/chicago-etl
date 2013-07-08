module Chicago
  module Flow
    class Error < RuntimeError
    end
    
    class RaisingErrorHandler
      def unregistered_sinks(sinks)
        raise Error.new("Sinks not registered: #{sinks.join(",")}")
      end
    end

    class PipelineStage
      attr_reader :transformation_chain
      
      def initialize(options={})
        @sinks  = options[:sinks] || {}
        @transformations = options[:transformations] || []
        @error_handler = options[:error_handler] || RaisingErrorHandler.new
        @transformation_chain = TransformationChain.new(*@transformations)
      end

      # Returns the named sink, if it exists
      def sink(name)
        @sinks[name.to_sym]
      end

      def register_sink(name, sink)
        @sinks[name.to_sym] = sink
        self
      end
      
      def validate_pipeline
        unless unregistered_sinks.empty?
          @error_handler.unregistered_sinks(unregistered_sinks)
        end
      end
      
      def execute(source)
        validate_pipeline
        @sinks.values.each(&:open)
        source.each do |row|
          transformation_chain.process(row).each {|row| process_row(row) }
        end
        transformation_chain.flush.each {|row| process_row(row) }
        @sinks.values.each(&:close)
      end

      def required_sinks
        transformation_chain.output_streams | [:default]
      end

      def unregistered_sinks
        required_sinks - @sinks.keys
      end
      
      private
      
      def process_row(row)
        stream = row.delete(:_stream) || :default
        @sinks[stream] << row
      end
    end
  end
end
