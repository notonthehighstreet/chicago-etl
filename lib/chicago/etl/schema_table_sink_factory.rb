module Chicago
  module ETL
    # Builds Sinks for Dimension & Fact tables.
    class SchemaTableSinkFactory
      # Creates a new factory.
      def initialize(db, schema_table)
        @db, @schema_table = db, schema_table
      end

      # Returns a sink to load data into the MySQL table backing the
      # schema table.
      #
      # Pass an :exclude option if you don't want all columns of the
      # schema table to be loaded via this sink.
      def sink(options={})
        MysqlFileSink.new(@db, @schema_table.table_name, 
                          mysql_options(options))
      end
      
      # Returns a sink to load data into the MySQL table backing the
      # key table for a Dimension.
      #
      # @option options [Symbol] :table - a custom key table name. The
      #   schema table's key table name will be used otherwise.
      def key_sink(options={})
        table = options.delete(:table) || @schema_table.key_table_name

        sink = MysqlFileSink.new(@db, table, mysql_options(options)).
          set_columns(:original_id, :dimension_id)

        sink.truncation_strategy = lambda do
          # No Op - we want to maintain keys to avoid having to sort
          # out fact tables.
        end
        sink
      end
      
      # Returns a sink to load errors generated in the ETL process.
      def error_sink(options={})
        sink = MysqlFileSink.new(@db, :etl_error_log, mysql_options(options)).
          set_columns(:column, :row_id, :error, :severity, :error_detail).
          set_constant_values(:table => @schema_table.table_name.to_s,
                              :process_name => "StandardTransformations",
                              :process_version => 3,
                              :logged_at => Time.now)

        sink.truncation_strategy = lambda do
          @db[:etl_error_log].
            where(:table => @schema_table.table_name.to_s).delete
        end
        sink
      end
      
      private

      def mysql_options(options)
        [:filepath, :ignore].inject({}) do |hsh, k|
          hsh[k] = options[k] if options.has_key?(k)
          hsh
        end
      end
    end
  end
end
