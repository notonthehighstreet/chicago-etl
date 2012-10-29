module Chicago
  module ETL
    module Screens
      class ColumnScreen
        attr_reader :column, :table_name
        
        def initialize(table_name, column)
          @table_name = table_name
          @column = column
          @error_name = self.class.name.split('::').last.sub(/Screen$/,'').titlecase
        end

        def self.for_columns(table_name, columns)
          screens = columns.map {|column| new(table_name, column) }
          CompositeScreen.new(screens)
        end

        def call(row, errors=[])
          value = row[column.name]

          if applies?(value)
            overwrite_value(row)
            log_error(value, errors)
          end

          [row, errors]
        end

        def severity
          1
        end

        private

        def overwrite_value(row)
          row[column.name] = column.default_value
        end

        def log_error(value, errors)
          errors << error_hash(value)
        end

        def error_hash(value)
          {
            :process_name => "StandardTransformations",
            :process_version => 2,
            :table => table_name.to_s,
            :column => column.name.to_s,
            :severity => severity,
            :error => @error_name
          }
        end

        def applies?(value)
        end
      end
    end
  end
end
