module Chicago
  module ETL
    # The destination for rows passing through a pipeline stage.
    #
    # @api public
    # @abstract
    class Sink < PipelineEndpoint
      # Specifies a hash of values that are assumed to apply to all
      # rows.
      #
      # Subclasses should use there constant values appropriately when
      # writing rows, by merging them with the row or otherwise
      # ensuring that they end up in the final source this sink
      # represents.
      def constant_values
        @constant_values ||= {}
      end

      # Sets a number of constant values.
      def set_constant_values(hash={})
        constant_values.merge!(hash)
        self
      end

      # Performs any operations before writing rows to this sink.
      #
      # By default does nothing; may be overridden by subclasses.
      def open
      end

      # Performs any operations after writing rows to this sink.
      #
      # By default does nothing; may be overridden by subclasses.
      def close
      end

      # Writes a row to this sink.
      #
      # By default does nothing; may be overridden by subclasses.
      def <<(row)
      end

      # Removes all rows from this sink.
      #
      # This includes all rows written prior to this particular
      # execution of a pipeline stage.
      #
      # By default does nothing; should be overritten by subclasses.
      def truncate
      end
    end
  end
end
