require 'sequel'
require 'sequel/load_data_infile'
require 'tmpdir'

Sequel.extension :core_extensions

module Chicago
  module ETL
    # @api public
    class MysqlFileSink < Sink
      attr_reader :filepath
      attr_writer :truncation_strategy

      def initialize(db, table_name, options = {})
        @filepath = options[:filepath] || temp_file(table_name)
        @serializer = MysqlFileSerializer.new
        @db = db
        @table_name = table_name
        @insert_ignore = !!options[:ignore]
      end

      def set_columns(*columns)
        @columns = [columns].flatten
        self
      end

      def name
        @table_name
      end

      def <<(row)
        csv << columns.map {|c| @serializer.serialize(row[c]) }
      end

      def close
        csv.flush
        load_from_file(filepath)
        csv.close
        File.unlink(filepath) if File.exists?(filepath)
      end

      # Loads data from the file into the MySQL table via LOAD DATA
      # INFILE, if the file exists and has content.
      def load_from_file(file)
        return unless File.size?(file)
        dataset.load_csv_infile(file, @columns, :set => constant_values)
      end

      def truncate
        if @truncation_strategy
          @truncation_strategy.call
        else
          @db[@table_name].truncate
        end
      end

      private

      def dataset
        @insert_ignore ? @db[@table_name].insert_ignore : @db[@table_name]
      end

      def csv
        @csv ||= CSV.open(filepath, "w")
      end

      def temp_file(table_name)
        File.join(Dir.tmpdir, "#{table_name}.#{rand(1_000_000)}.csv")
      end
    end
  end
end
