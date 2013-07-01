module Chicago
  module ETL
    class DatasetBatchStage
      def initialize(dataset, pipeline_stage)
        @dataset = dataset
        @pipeline_stage = pipeline_stage
      end

      def execute(etl_batch, reextract=false)
        @pipeline_stage.sink(:default).
          constant_values[:_inserted_at] = Time.now
        @pipeline_stage.execute(source(etl_batch, reextract))
      end

      private

      def source(etl_batch, reextract=false)
        if reextract
          filtered_dataset = @dataset
        else
          filtered_dataset = @dataset.filter_to_etl_batch(etl_batch)
        end
        Chicago::Flow::DatasetSource.new(filtered_dataset)
      end
    end
  end
end
