module Chicago
  module ETL
    module Transformations
      # Filters rows so they only get output once, based on a :key.
      class WrittenRowFilter < Flow::Transformation
        def initialize(*args)
          super(*args)
          @written_rows = Set.new
        end

        def process_row(row)
          key = row[@options[:key]]

          unless @written_rows.include?(key)
            @written_rows << key
            row
          end
        end
      end

      # Adds an :id field to a row, based on a KeyBuilder.
      #
      # Also adds this id as :row_id to any rows in an embedded
      # :_errors field.
      #
      # Pass the :key_builder option to set the KeyBuilder.
      class AddKey < Flow::Transformation
        adds_fields :id

        def process_row(row)
          row[:id] ||= @options[:key_builder].key(row)
          (row[:_errors] || []).each {|e| e[:row_id] = row[:id] }
          row
        end
      end

      # Removes embedded :_errors and puts them on the error stream.
      class DemultiplexErrors < Flow::Transformation
        def output_streams
          [:default, :error]
        end

        def process_row(row)
          errors = (row.delete(:_errors) || []).each do |e|
            assign_stream(e, :error)
          end

          [row] + errors
        end
      end
    end
  end
end
