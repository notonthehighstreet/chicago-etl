module Chicago
  module ETL
    # A Source or a Sink.
    #
    # @api public
    # abstract
    class StageEndpoint
      attr_reader :columns

      def has_defined_columns?
        !columns.empty?
      end
    end
  end
end
