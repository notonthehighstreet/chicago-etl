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

        ensure_options_present
      end

      def self.required_options
        @required_options ||= []
      end

      def self.added_fields
        @added_fields ||= []
      end

      def self.removed_fields
        @removed_fields ||= []
      end

      def self.requires_options(*options)
        required_options.concat options.flatten
      end

      def self.adds_fields(*fields)
        added_fields.concat fields.flatten
      end

      def self.removes_fields(*fields)
        removed_fields.concat fields.flatten
      end
      
      def required_options
        self.class.required_options
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

      private

      def ensure_options_present
        missing_keys = required_options - @options.keys

        unless missing_keys.empty?
          raise ArgumentError.new("The following options are not supplied: " + missing_keys.join(","))
        end
      end
    end
  end
end
