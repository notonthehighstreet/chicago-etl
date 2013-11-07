module Chicago
  module ETL
    class DeduplicateRows < Chicago::Flow::Transformation
      def process_row(row)
        if @working_row.nil?
          @working_row = row
          return
        elsif same_row?(row)
          @working_row = merge_rows(row)
          return
        else
          assign_new_row_and_return_old_row(row)
        end
      end

      def flush
        @working_row.nil? ? [] : [@working_row]
      end

      protected

      attr_reader :working_row

      # This should be implemented by clients
      def merge_rows(row)
      end
      
      # This should be implemented by clients
      def same_row?(row)
      end

      private

      def assign_new_row_and_return_old_row(row)
        row, @working_row = @working_row, row
        row
      end
    end
  end
end
