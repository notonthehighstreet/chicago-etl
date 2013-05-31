module Chicago
  module Flow
    class ArraySink < Sink
      attr_reader :data
      
      def initialize(fields=[])
        @fields = [fields].flatten
        @data = []
      end

      def open
      end
      
      def <<(row)
        @data << row.merge(constant_values)
      end

      def close
      end
    end
  end
end
