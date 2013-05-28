module Chicago
  module Flow
    class MysqlFileSink
      attr_reader :fields
      
      def initialize(table, filepath, fields)
        @filepath = filepath
        @fields = fields
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

      def has_defined_fields?
        true
      end
    end
  end
end
