module Chicago
  module Flow
    # An endpoint that stores rows in an Array.
    #
    # @api public
    class ArraySink < Sink
      # Returns the array of written rows.
      attr_reader :data

      # The name of this sink
      attr_reader :name
      
      # Creates an ArraySink.
      #
      # Optionally you may pass an array of column names if you wish
      # to use static validation that the correct columns are written
      # through the pipeline.
      def initialize(name, fields=[])
        @name = name
        @fields = [fields].flatten
        @data = []
      end

      # See Sink#<<
      def <<(row)
        @data << row.merge(constant_values)
      end

      # See Sink#truncate
      def truncate
        @data.clear
      end
    end
  end
end
