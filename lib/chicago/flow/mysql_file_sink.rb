module Chicago
  module Flow
    class MysqlFileSink < PipelineEndpoint
      def initialize(table, filepath, fields)
        @fields = [fields].flatten
        @filepath = filepath
        @serializer = MysqlFileSerializer.new
      end

      def open
        @csv = CSV.open(@filepath)
      end

      def <<(row)
        open unless @csv
        @csv << fields.map {|c| @serializer.serialize(row[c]) }
      end

      def close
        @csv.close
      end
    end
  end
end
