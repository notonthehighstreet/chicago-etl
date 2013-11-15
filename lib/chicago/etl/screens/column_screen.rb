module Chicago
  module ETL
    module Screens
      # @abstract
      class ColumnScreen < Transformation
        def self.for_columns(columns)
          columns.map {|column|
            new(:default, :column => column) 
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
