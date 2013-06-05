require 'chicago/etl/sink'

module Chicago
  module ETL
    # Wrapper around a dataset to allowed buffered inserts.
    #
    # @api public
    class BufferingInsertWriter
      # The number of rows written before inserting to the DB.
      BUFFER_SIZE = 10_000
      
      # Returns the column names expected to be written to this sink.
      # @api public
      attr_reader :column_names

      def initialize(dataset, column_names)
        @output = []
        @dataset = dataset
        @column_names = column_names
      end
      
      # Writes a row to the output.
      #
      # Row will not be written to the output if it has already been
      # written, as identified by the unique row key.
      #
      # Should not be overridden by subclasses - overwrite write instead.
      def <<(row)
        write row
      end

      def flush
        @dataset.insert_replace.import(column_names, output)
        @output.clear
      end

      protected

      def write(row)
        @output << @column_names.map {|name| row[name] }
        flush if reached_buffer_limit?
      end

      private

      def reached_buffer_limit?
        @output.size >= BUFFER_SIZE
      end
    end
  end
end
