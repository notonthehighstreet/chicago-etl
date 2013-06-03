module Chicago
  module Flow
    class Sink < PipelineEndpoint
      def constant_values
        @constant_values ||= {}
      end

      def set_constant_value(field, value)
        constant_values[field] = value
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
    end
  end
end
