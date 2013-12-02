module Chicago
  module ETL
    # A stage that performs in-table updates.
    #
    # Source is currently expected to be a Dataset.
    class DirectUpdateStage < BasicStage
      attr_reader :source

      def initialize(name, options={})
        super(name, options)
        @source = options[:source]
        @updates = options[:updates]
        @filter_strategy = options[:filter_strategy] || lambda {|s, _| s }
      end

      def perform_execution(etl_batch)
        filtered_source(etl_batch).dataset.update(@updates)
      end

      # @api private
      def filtered_source(etl_batch)
        filtered_dataset = etl_batch.reextracting? ? source : 
          @filter_strategy.call(source, etl_batch)

        DatasetSource.new(filtered_dataset)
      end
    end
  end
end
