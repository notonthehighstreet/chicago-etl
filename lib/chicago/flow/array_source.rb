module Chicago
  module Flow
    class ArraySource
      attr_reader :fields
      
      def initialize(array, fields=[])
        @array = array
        @fields = [fields].flatten
      end

      def each
        @array.each {|row| yield row }
      end

      def has_defined_fields?
        !fields.empty?
      end
    end
  end
end
