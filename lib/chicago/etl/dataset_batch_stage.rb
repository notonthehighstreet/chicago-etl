module Chicago
  module ETL
    # Allows deferring constructing a DatasetSource until extract
    # time, so that it can be filtered to an ETL batch appropriately.
    class DatasetBatchStage < Stage
      # Executes this ETL stage.
      #
      # Configures the dataset and flows rows into the pipeline.
      def execute(etl_batch, reextract=false)
        if reextract && sink(:error) && !truncate_pre_load?
          sink(:error).truncate
        end
        
        sink(:default).set_constant_values(:_inserted_at => Time.now)
        super
      end
    end
  end
end
