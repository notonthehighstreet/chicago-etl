module Chicago
  module Flow
    class MysqlFileSink
      def initialize(table, filepath, columns)
        @filepath = filepath
        @columns = columns
        @serializer = MysqlFileSerializer.new
      end

      def open
        @csv = CSV.open(@filepath)
      end

      def <<(row)
        open unless @csv
        @csv << @columns.map {|c| @serializer.serialize(row[c]) }
      end

      def close
        @csv.close
      end
    end
  end
end
