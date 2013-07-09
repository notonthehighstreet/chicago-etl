module Chicago
  module Flow
    # @api public
    class ArraySource < PipelineEndpoint
      def initialize(array, fields=[])
        @fields = [fields].flatten
        @array = array
      end

      def each
        @array.each {|row| yield row }
      end
    end
  end
end
