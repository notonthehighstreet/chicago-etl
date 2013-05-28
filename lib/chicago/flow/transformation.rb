module Chicago
  module Flow
    STREAM = :_stream

    class Transformation
      def initialize(*args)
        stream, options = *args
        if stream.kind_of?(Hash)
          @stream = :default
          @options = stream
        else
          @stream = stream || :default
          @options = options || {}
        end
      end

      class << self
        attr_reader :added_fields, :removed_fields

        def adds_fields(*fields)
          @added_fields ||= []
          @added_fields += fields.flatten
        end

        def removes_fields(*fields)
          @removed_fields ||= []
          @removed_fields += fields.flatten
        end
      end
      
      def added_fields
        self.class.added_fields
      end

      def removed_fields
        self.class.removed_fields
      end
      
      def upstream_fields(fields)
        ((fields + removed_fields) - added_fields).uniq
      end

      def downstream_fields(fields)
        ((fields - removed_fields) + added_fields).uniq
      end
      
      def process(row)
        applies_to_stream?(row[STREAM]) ? process_row(row) : row
      end

      def flush
        []
      end
      
      def output_streams
        [:default]
      end

      def applies_to_stream?(target_stream)
        @stream == :all ||
          (target_stream.nil? && @stream == :default) ||
          target_stream == @stream
      end
      
      protected

      def process_row(row)
        row
      end
      
      def assign_stream(row, stream)
        raise "Stream not declared" unless stream.nil? || output_streams.include?(stream)
        row[STREAM] = stream if stream
        row
      end
    end
  end
end
