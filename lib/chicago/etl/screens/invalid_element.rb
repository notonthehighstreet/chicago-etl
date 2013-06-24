module Chicago
  module ETL
    module Screens
      class InvalidElement < ColumnScreen
        def self.for_columns(columns)
          columns.select(&:elements).map {|column| new(:default, :column => column) }
        end

        def severity
          3
        end

        def applies?(value)
          column.elements && 
            !column.elements.map(&:downcase).include?(value.to_s.downcase)
        end

        def error(value)
          super(value).
            merge(:error_detail => "'#{value}' is not a valid value.")
        end
      end
    end
  end
end
