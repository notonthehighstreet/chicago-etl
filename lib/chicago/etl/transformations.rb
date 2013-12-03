module Chicago
  module ETL
    module Transformations
      # Filters rows so they only get output once, based on a :key.
      class WrittenRowFilter < Transformation
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
      class AddKey < Transformation
        requires_options :key_builder
        adds_columns :id

        def output_streams
          [:default, :dimension_key]
        end

        def process_row(row)
          key, key_row = key_builder.key(row)
          row[:id] = key
          (row[:_errors] || []).each {|e| e[:row_id] = row[:id] }

          if key_row
            assign_stream(key_row, :dimension_key)
            [row, key_row]
          else
            row
          end
        end

        def key_builder
          @options[:key_builder]
        end
      end

      # Removes embedded :_errors and puts them on the error stream.
      class DemultiplexErrors < Transformation
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

      # Removes a field from the row, and creates a row on a
      # designated key stream
      class DimensionKeyMapping < Transformation
        requires_options :original_key, :key_table

        def removed_columns
          [original_key]
        end

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

      # Adds a hash of the specified columns as a field in the row.
      class HashColumns < Transformation
        requires_options :columns

        def process_row(row)
          str = hash_columns.map {|c| row[c].to_s }.join
          row.put(output_field, Digest::MD5.hexdigest(str).upcase)
        end

        def added_columns
          [output_field]
        end

        def output_field
          @options[:output_field] || :hash
        end

        def hash_columns
          @options[:columns]
        end
      end
    end
  end
end
