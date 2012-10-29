require 'chicago/etl/sink'

module Chicago
  module ETL
    # Wrapper around FasterCSV's output object, to convert values to a
    # format required by MySQL's LOAD DATA INFILE command.
    #
    # @api public
    class MysqlDumpfile < Sink
      # Creates a new writer.
      #
      # @param csv a FasterCSV output object
      # @param [Symbol] column_names columns to be output
      # @param key an optional key to ensure rows are written only once.
      def initialize(csv, column_names, key=nil)
        super(csv, column_names, key)
        @transformer = MysqlLoadFileValueTransformer.new
      end

      protected

      # Writes a row to the output.
      #
      # @param Hash row Only keys in column_names will be output.
      def write(row)
        output << @column_names.map {|name| 
          @transformer.transform(row[name]) 
        }
      end
    end
  end
end
