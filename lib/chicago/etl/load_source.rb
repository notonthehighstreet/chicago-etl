module Chicago
  module ETL
    class LoadSource
      attr_reader :name

      def initialize(name, &block)
        @name = name
        @dataset_builder = block
      end

      # Returns a dataset, given a database connection.
      def dataset(db)
        block.call(db)
      end
    end
  end
end
