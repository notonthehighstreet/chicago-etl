module Chicago
  module ETL
    # @api public
    class ArraySource < StageEndpoint
      def initialize(array, columns=[])
        @columns = [columns].flatten
        @array = array
      end

      def each
        @array.each {|row| yield row }
      end
    end
  end
end
