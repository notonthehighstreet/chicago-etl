module Chicago
  module Flow
    class ArraySink < Sink
      attr_reader :data, :name
      
      def initialize(name, fields=[])
        @name = name
        @fields = [fields].flatten
        @data = []
      end

      def <<(row)
        @data << row.merge(constant_values)
      end

      def truncate
        @data.clear
      end
    end
  end
end
