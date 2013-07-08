require 'sequel'
require 'sequel/fast_columns'

module Chicago
  module Flow
    class DatasetSource < PipelineEndpoint
      attr_reader :dataset

      def initialize(dataset)
        @dataset = dataset
      end

      def each
        @dataset.each {|row| yield row }
      end

      def fields
        @dataset.columns
      end
    end
  end
end
