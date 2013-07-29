module Chicago
  module ETL
    # Links a PipelineStage to a Dataset.
    #
    # Allows deferring constructing a DatasetSource until extract
    # time, so that it can be filtered to an ETL batch appropriately.
    class DatasetBatchStage
      attr_reader :name

      def initialize(name, dataset, pipeline_stage, options={})
        @name = name
        @dataset = dataset
        @pipeline_stage = pipeline_stage
        @filter_strategy = options[:filter_strategy] || lambda {|dataset, etl_batch|
          dataset.filter_to_etl_batch(etl_batch)
        }
        @truncate_pre_load = !!options[:truncate_pre_load]
      end

      # Executes this ETL stage.
      #
      # Configures the dataset and flows rows into the pipeline.
      def execute(etl_batch, reextract=false)
        if @truncate_pre_load
          pipeline_stage.sinks.each {|sink| sink.truncate }
        elsif reextract && pipeline_stage.sink(:error)
          pipeline_stage.sink(:error).truncate
        end

        pipeline_stage.execute(source(etl_batch, reextract))
      end

      # Returns the pipeline for this stage.
      def pipeline_stage
        @pipeline_stage.sink(:default).
          set_constant_values(:_inserted_at => Time.now)
        @pipeline_stage
      end

      # Returns a DatasetSource for the provided dataset filtered to
      # the ETL batch as appropriate.
      def source(etl_batch, reextract=false)
        if reextract
          filtered_dataset = @dataset
        else
          filtered_dataset = @filter_strategy.call(@dataset, etl_batch)
        end
        Chicago::Flow::DatasetSource.new(filtered_dataset)
      end
    end
  end
end
