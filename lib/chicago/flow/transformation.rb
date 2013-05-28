module Chicago
  module Flow
    STREAM = :_stream

    class Transformation
      def initialize(*args)
        stream, options = *args
        if stream.kind_of?(Hash)
          @stream = :default
          @options = stream
        else
          @stream = stream || :default
          @options = options || {}
        end
      end
      
      def process(row)
        applies_to_stream?(row[STREAM]) ? process_row(row) : row
      end

      def flush
        []
      end
      
      def output_streams
        [:default]
      end

      def applies_to_stream?(target_stream)
        @stream == :all ||
          (target_stream.nil? && @stream == :default) ||
          target_stream == @stream
      end
      
      protected

      def process_row(row)
        row
      end
      
      def assign_stream(row, stream)
        raise "Stream not declared" unless stream.nil? || output_streams.include?(stream)
        row[STREAM] = stream if stream
        row
      end
    end
  end
end
