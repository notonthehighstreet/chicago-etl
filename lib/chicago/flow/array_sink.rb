module Chicago
  module Flow
    class ArraySink
      attr_reader :data
      
      def initialize(fields=[])
        @fields = [fields].flatten
        @data = []
      end

      def open
      end
      
      def <<(row)
        @data << row
      end

      def close
      end
    end
  end
end
