require 'sequel'
require 'sequel/load_data_infile'
require 'tempfile'

module Chicago
  module Flow
    class MysqlFileSink < Sink
      attr_reader :filepath

      def initialize(db, table_name, fields, options = {})
        @fields = [fields].flatten
        @filepath = options[:filepath] || Tempfile.new(table_name.to_s).path
        @serializer = MysqlFileSerializer.new
        @db = db
        @table_name = table_name
        @insert_ignore = !!options[:ignore]
      end

      def <<(row)
        csv << fields.map {|c| @serializer.serialize(row[c]) }
      end

      def close
        csv.close
        load_from_file(filepath)
        File.unlink(filepath) if File.exists?(filepath)
      end

      # Loads data from the file into the MySQL table via LOAD DATA
      # INFILE, if the file exists and has content.
      def load_from_file(file)
        return unless File.size?(file)        
        dataset.load_csv_infile(file, @fields, :set => constant_values)
      end

      private

      def dataset
        @insert_ignore ? @db[@table_name].insert_ignore : @db[@table_name]
      end

      def csv
        @csv ||= CSV.open(filepath)
      end
    end
  end
end
