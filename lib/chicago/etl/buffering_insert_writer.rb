require 'chicago/etl/sink'

module Chicago
  module ETL
    # Wrapper around a dataset to allowed buffered inserts.
    #
    # @api public
    class BufferingInsertWriter < Sink
      # The number of rows written before inserting to the DB.
      BUFFER_SIZE = 10_000
      
      def initialize(dataset, column_names, key=nil)
        super([], column_names, key)
        @dataset = dataset
      end
      
      def flush
        @dataset.insert_replace.multi_insert(output)
        output.clear
      end

      protected

      def write(row)
        output << @column_names.map {|name| row[name] }
        flush if reached_buffer_limit?
      end

      private

      def reached_buffer_limit?
        output.size >= BUFFER_SIZE
      end
    end
  end
end
