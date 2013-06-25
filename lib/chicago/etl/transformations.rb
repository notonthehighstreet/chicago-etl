module Chicago
  module ETL
    module Transformations
      # Filters rows so they only get output once, based on a :key.
      class WrittenRowFilter < Flow::Transformation
        requires_options :key

        def initialize(*args)
          super(*args)
          @written_rows = Set.new
        end
        
        def process_row(row)
          key = row[key_field]
          # puts "Checking on #{key}"
          unless @written_rows.include?(key)
            @written_rows << key
            row
          end
        end

        def key_field
          @options[:key]
        end
      end

      # Adds an :id field to a row, based on a KeyBuilder.
      #
      # Also adds this id as :row_id to any rows in an embedded
      # :_errors field.
      #
      # Pass the :key_builder option to set the KeyBuilder.
      class AddKey < Flow::Transformation
        requires_options :key_builder
        adds_fields :id

        def process_row(row)
          row[:id] ||= key_builder.key(row)
          # puts "Key assigned: #{row[:id]}"
          (row[:_errors] || []).each {|e| e[:row_id] = row[:id] }
          row
        end

        def key_builder
          @options[:key_builder]
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

      class DimensionKeyMapping < Flow::Transformation
        requires_options :original_key, :key_table

        def output_streams
          [:default, key_table]
        end

        def process_row(row)
          key_row = {
            :original_id => row.delete(original_key),
            :dimension_id => row[:id]
          }
          assign_stream(key_row, key_table)
          [row, key_row]
        end

        def original_key
          @options[:original_key]
        end

        def key_table
          @options[:key_table]
        end
      end
    end
  end
end
