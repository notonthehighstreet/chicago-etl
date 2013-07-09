module Chicago
  module Flow
    # @api public
    # abstract
    class PipelineEndpoint
      attr_reader :fields

      def has_defined_fields?
        !fields.empty?
      end
    end
  end
end
