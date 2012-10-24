module Chicago
  module ETL
    module Screens
      class OutOfBounds < ColumnScreen
        def severity
          2
        end

        def applies?(value)
          return false unless value

          (column.numeric? && applies_to_numeric?(value)) ||
            (column.column_type == :string && applies_to_string?(value))
        end

        def overwrite_value(row)
        end

        private

        def applies_to_numeric?(value)
          (column.min && value < column.min) || 
            (column.max && value > column.max)
        end

        def applies_to_string?(value)
          (column.min && value.length < column.min) || 
            (column.max && value.length > column.max)
        end
      end
    end
  end
end
