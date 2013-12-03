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

      # Returns the columns added by this transformation.
      def self.added_columns
        @added_columns ||= []
      end

      # Returns the columns removed by this transformation.
      def self.removed_columns
        @removed_columns ||= []
      end

      # Specify which options are required in the constructor of
      # this transformation.
      def self.requires_options(*options)
        required_options.concat options.flatten
      end

      # Specify which columns are added to the row by this
      # transformation.
      def self.adds_columns(*columns)
        added_columns.concat columns.flatten
      end

      # Specify which columns are removed from the row by this
      # transformation.
      def self.removes_columns(*columns)
        removed_columns.concat columns.flatten
      end
      
      # Returns the required initialization options for this transformation.
      def required_options
        self.class.required_options
      end
      
      # Returns the columns added by this transformation.
      def added_columns
        self.class.added_columns
      end

      # Returns the columns removed by this transformation.
      def removed_columns
        self.class.removed_columns
      end
      
      def upstream_columns(columns)
        ((columns + removed_columns) - added_columns).uniq
      end

      def downstream_columns(columns)
        ((columns - removed_columns) + added_columns).uniq
      end

      # Processes a row if the row is on this transformation's stream.
      #
      # This should not be overridden by subclasses, override
      # process_row instead.
      #
      # @return Hash if a single row is returned
      # @return Array<Hash> if multiple rows need to be returned
      def process(row)
        applies_to_stream?(row[STREAM]) ? process_row(row) : row
      end

      # Returns all remaining rows yet to make their way through the
      # pipeline.
      #
      # This should be overridden by subclasses if the transformation
      # holds back rows as it does processing (to find the maximum
      # value in a set of rows for example), to ensure that all rows
      # are written through the pipeline.
      #
      # @return Array<Hash> by default an empty array.
      def flush
        []
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
    end
  end
end
