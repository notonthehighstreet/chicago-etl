require 'sequel'
require 'sequel/load_data_infile'

module Chicago
  module Flow
    class MysqlFileSink < Sink
      def initialize(db, target_table, filepath, fields)
        @fields = [fields].flatten
        @filepath = filepath
        @serializer = MysqlFileSerializer.new
        @db = db
        @target_table = target_table
      end

      def open
      end

      def <<(row)
        csv << fields.map {|c| @serializer.serialize(row[c]) }
      end

      def close
        csv.close
        load_file_into_table
        File.unlink(@filepath) if File.exists?(@filepath)
      end

      private

      def csv
        @csv ||= CSV.open(@filepath)
      end

      def load_file_into_table
        @db[@target_table].insert_ignore.load_csv_infile(@filepath, @fields)
      end
    end
  end
end
