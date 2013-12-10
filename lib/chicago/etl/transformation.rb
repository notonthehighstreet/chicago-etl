module Chicago
  module ETL
    # The key used to store the stream in the row.
    #
    # @api private
    STREAM = :_stream

    # A base class for row transformations.
    #
    # Transformations process hash-like rows by filtering or altering
    # their contents.
    #
    # @api public
    # @abstract Subclass and add a process_row method
    class Transformation
      # Creates the transformation.
      #
      # This should not be overridden by subclasses - transformations
      # that need their own arguments should do so by passing named
      # options.
      #
      # @overload initialize(stream, options)
      #   Specifies this transformation applies to a specific
      #   stream. Options are specific to the stream subclass
      # @overload initialize(options)
      #   As above, but the stream is assumed to be :default
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

      # Returns the required initialization options for this transformation.
      def self.required_options
        @required_options ||= []
      end

      # Returns the fields added by this transformation.
      def self.added_fields
        @added_fields ||= []
      end

      # Returns the fields removed by this transformation.
      def self.removed_fields
        @removed_fields ||= []
      end

      # Specify which options are required in the constructor of
      # this transformation.
      def self.requires_options(*options)
        required_options.concat options.flatten
      end

      # Specify which fields are added to the row by this
      # transformation.
      def self.adds_fields(*fields)
        added_fields.concat fields.flatten
      end

      # Specify which fields are removed from the row by this
      # transformation.
      #
      # Fields will be removed automatically; subclasses don't need to
      # remove them.
      def self.removes_fields(*fields)
        removed_fields.concat fields.flatten
      end
      
      # Returns the required initialization options for this transformation.
      def required_options
        self.class.required_options
      end
      
      # Returns the fields added by this transformation.
      def added_fields
        self.class.added_fields
      end

      # Returns the fields removed by this transformation.
      def removed_fields
        self.class.removed_fields
      end
      
      def upstream_fields(fields)
        ((fields + removed_fields) - added_fields).uniq
      end

      def downstream_fields(fields)
        ((fields - removed_fields) + added_fields).uniq
      end

      # Processes a row if the row is on this transformation's stream.
      #
      # This should not be overridden by subclasses, override
      # process_row instead.
      #
      # @return [Hash]
      # @return Array<Hash> if multiple rows need to be returned
      def process(row)
        if applies_to_stream?(row[STREAM]) 
          ensure_fields_removed process_row(row)
        else 
          [row]
        end
      end

      # Returns all remaining rows yet to make their way through the
      # pipeline.
      #
      # This should *not* be overridden by subclasses - override
      # flush_rows instead.
      #
      # @return Array<Hash> by default an empty array.
      def flush
        ensure_fields_removed flush_rows
      end

      # Returns the streams to which this transformation may write
      # rows.
      #
      # By default, transformations are assumed to write only to the
      # :default stream. Override this in subclasses as necessary.
      def output_streams
        [:default]
      end

      # Returns true if this transformation should be applied to a row
      # on the target stream.
      def applies_to_stream?(target_stream)
        @stream == :all ||
          (target_stream.nil? && @stream == :default) ||
          target_stream == @stream
      end
      
      protected

      # Performs transformation on the row.
      #
      # By default does nothing; override in subclasses. Subclasses
      # should return either nil, a Hash-like row or an Array of
      # Hash-like rows.
      def process_row(row)
        row
      end

      # Returns all remaining rows yet to make their way through the
      # pipeline.
      #
      # This should be overridden by subclasses if the transformation
      # holds back rows as it does processing (to find the maximum
      # value in a set of rows for example), to ensure that all rows
      # are written through the pipeline.
      def flush_rows
      end
      
      # Assigns the row to a stream.
      #
      # Will raise an error if the stream is not declared by
      # overriding output_streams.
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

      def ensure_fields_removed(rows)
        [rows].flatten.compact.each do |row|
          remove_fields(row) if applies_to_stream?(row[STREAM])
        end
      end

      def remove_fields(row)
        removed_fields.each {|field| row.delete(field) }
      end
    end
  end
end
