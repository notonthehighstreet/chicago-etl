module Chicago
  module ETL
    module Transformations
      class AddEtlBatchId
        def initialize(etl_batch_id)
          @etl_batch_id = etl_batch_id
        end

        def call(row, errors=[])
          row[:etl_batch_id] = @etl_batch_id
          [row, errors]
        end
      end
    end
  end
end

