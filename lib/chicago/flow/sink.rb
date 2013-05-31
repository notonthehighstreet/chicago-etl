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
    end
  end
end
