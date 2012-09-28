module Chicago
  module ETL
    module Transformations
      class AddBatchId
        def initialize(etl_batch_id)
          @etl_batch_id = etl_batch_id
        end

        def call(errors, row)
          [errors, [row.merge(:etl_batch_id => @etl_batch_id)]]
        end
      end
    end
  end
end
