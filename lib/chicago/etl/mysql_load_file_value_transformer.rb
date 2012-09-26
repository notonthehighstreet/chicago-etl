module Chicago
  module ETL
    class MysqlLoadFileValueTransformer
      # Transforms a value to be suitable for use in file in a LOAD
      # DATA INFILE mysql statement.
      def transform(value)
        case value
        when nil
          "\\N"
        when true
          "1"
        when false
          "0"
        when Time, DateTime
          value.strftime("%Y-%m-%d %H:%M:%S")
        when Date
          value.strftime("%Y-%m-%d")
        else
          value
        end
      end
    end
  end
end
