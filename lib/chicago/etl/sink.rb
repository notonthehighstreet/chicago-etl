require 'set'

module Chicago
  module ETL
    # An end point to write rows.
    #
    # @abstract
    # @api public
    class Sink
      # Returns the column names expected to be written to this sink.
      # @api public
      attr_reader :column_names

      # @abstract
      def initialize(output, column_names, unique_row_key=nil)
        @output = output
        @column_names = column_names
        @written_rows = Set.new
        @unique_row_key = unique_row_key
      end

      # Writes a row to the output.
      #
      # Row will not be written to the output if it has already been
      # written, as identified by the unique row key.
      #
      # Should not be overridden by subclasses - overwrite write instead.
      def <<(row)
        unless written?(row)
          write row
          @written_rows << row[@unique_row_key]
        end
      end
      
      # Flushes any remaining writes to the output.
      #
      # By default does nothing, subclasses should override where
      # necessary.
      def flush
      end

      # Returns true if this row has previously been written to the
      # output.
      #
      # Always returns false if no key to determine row uniqueness has
      # been provided.
      def written?(row)
        return false if @unique_row_key.nil?
        @written_rows.include?(row[@unique_row_key])
      end

      protected

      attr_reader :output

      # @abstract
      def write(row)
      end
    end
  end
end
