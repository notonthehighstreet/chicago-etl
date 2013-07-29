module Chicago
  module ETL
    # Builds sinks for Dimension & Fact tables.
    class SchemaTableSinkFactory
      def initialize(db, schema_table)
        @db, @schema_table = db, schema_table
      end

      # Returns a sink to load data into the MySQL table backing the
      # schema table.
      #
      # Pass an :exclude option if you don't want all columns of the
      # schema table to be loaded via this sink.
      def sink(options={})
        Flow::MysqlFileSink.new(@db,
                                @schema_table.table_name,
                                load_columns(options[:exclude]),
                                mysql_options(options))
      end
      
      # Returns a sink to load data into the MySQL table backing the
      # key table for a Dimension.
      def key_sink(options={})
        table = options.delete(:table) || @schema_table.key_table_name
        sink = Flow::MysqlFileSink.new(@db,
                                       table,
                                       [:original_id, :dimension_id],
                                       mysql_options(options))
        sink.truncation_strategy = lambda do
          # No Op - we want to maintain keys to avoid having to sort
          # out fact tables.
        end
        sink
      end

      def error_sink(options={})
        sink = Flow::MysqlFileSink.
          new(@db, :etl_error_log, 
              [:column, :row_id, :error, :severity, :error_detail], mysql_options(options)).
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
      
      def load_columns(exclude=nil)
        exclude = [exclude].compact.flatten
        [:id] + @schema_table.columns.
          reject {|c| exclude.include?(c.name) }.
          map {|c| c.database_name }
      end

      def mysql_options(options)
        [:filepath, :ignore].inject({}) do |hsh, k|
          hsh[k] = options[k] if options.has_key?(k)
          hsh
        end
      end
    end
  end
end
