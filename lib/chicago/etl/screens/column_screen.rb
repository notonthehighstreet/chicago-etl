module Chicago
  module ETL
    module Screens
      class ColumnScreen < Flow::Transformation
        def self.for_columns(table_name, columns)
          columns.map {|column|
            new(:default, :table_name => table_name, :column => column) 
          }
        end

        def output_streams
          [:default, :error]
        end

        def process_row(row)
          if applies?(row[column.database_name])
            overwrite_value(row)
            error_row = error(row[column.database_name])
            if error_row
              row[:_errors] ||= [] 
              row[:_errors] << error_row
            end
          end

          row
        end

        def severity
          1
        end

        def table_name
          @options[:table_name]
        end

        def column
          @options[:column]
        end

        private

        def error_name
          self.class.name.split('::').last.sub(/Screen$/,'').titlecase
        end

        def overwrite_value(row)
          row[column.database_name] = column.default_value
        end

        def error(value)
          {
            :process_name => "StandardTransformations",
            :process_version => 2,
            :table => table_name.to_s,
            :column => column.database_name.to_s,
            :severity => severity,
            :error => error_name
          }
        end

        def applies?(value)
        end
      end
    end
  end
end
