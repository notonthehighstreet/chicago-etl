require 'sequel'
require 'sequel/fast_columns'

module Chicago
  module ETL
    # @api public
    class DatasetSource < StageEndpoint
      attr_reader :dataset

      def initialize(dataset)
        @dataset = dataset
      end

      def each
        @dataset.each {|row| yield row }
      end

      def columns
        @dataset.columns
      end
    end
  end
end
