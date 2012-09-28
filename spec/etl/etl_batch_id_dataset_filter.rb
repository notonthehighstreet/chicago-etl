module Chicago
  module ETL
    class EtlBatchIdDatasetFilter
      def initialize(etl_batch_id)
        @etl_batch_id = etl_batch_id
      end

      # Returns a new dataset, filtered by all tables where the etl
      # batch id matches.
      def filter(dataset)
        dataset.filter(conditions(filterable_tables(dataset)))
      end

      private

      def filterable_tables(dataset)
        dataset.dependant_tables.select {|t|
          dataset.db.schema(t).map(&:first).include?(:etl_batch_id)
        }
      end

      def conditions(tables)
        tables.
          map {|t| {:etl_batch_id.qualify(t) => @etl_batch_id} }.
          inject {|a,b| a | b}
      end
    end
  end
end
