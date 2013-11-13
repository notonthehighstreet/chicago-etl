module Chicago
  module ETL
    class DeduplicateRows < Chicago::Flow::Transformation
      def process_row(row)
        if @working_row.nil?
          @working_row = new_row(row)
          return
        elsif same_row?(row)
          @working_row = merge_rows(row)
          return
        else
          assign_new_row_and_return_old_row(row)
        end
      end

      def flush
        @working_row.nil? ? [] : [return_row(@working_row)]
      end

      protected

      # Returns the current working row.
      attr_accessor :working_row

      # This should be implemented by clients
      def merge_rows(row)
        row
      end
      
      # Called for every row to determine whether the row is part of
      # the same group as the current working row.
      #
      # This should be implemented by clients. By default, all rows
      # are considered different.
      def same_row?(row)
      end

      # Called whenever a new row is detected.
      # 
      # Default behavior is to return the row unmodified - this may be
      # overridden by clients - if it is then the method should return
      # a row.
      def new_row(row)
        row
      end

      # Called whenever a row is about to be returned downstream.
      # 
      # Default behavior is to return the row unmodified - this may be
      # overridden by clients - if it is then the method should return
      # a row.
      def return_row(row)
        row
      end

      private

      def assign_new_row_and_return_old_row(row)
        row = return_row(@working_row)
        @working_row = new_row(row)
        row
      end
    end
  end
end
