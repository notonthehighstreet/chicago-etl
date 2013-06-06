module Chicago
  module ETL
    module Screens
      class MissingValue < ColumnScreen
        def severity
          column.descriptive? ? 1 : 2
        end

        def error(value)
          if ! (column.column_type == :boolean || column.optional?)
            super(value)
          end
        end

        def applies?(value)
          value.nil? ||
            (column.column_type == :string && value.blank?)
        end
      end
    end
  end
end
