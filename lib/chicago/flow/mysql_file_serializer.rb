require 'date'

module Chicago
  module Flow
    # @api private
    class MysqlFileSerializer
      # Transforms a value to be suitable for use in file in a LOAD
      # DATA INFILE mysql statement.
      def serialize(value)
        case value
        when nil
          "NULL"
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
