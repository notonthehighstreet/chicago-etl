module Chicago
  module ETL
    # Links a PipelineStage to a Dataset.
    #
    # Allows deferring constructing a DatasetSource until extract
    # time, so that it can be filtered to an ETL batch appropriately.
    class DatasetBatchStage < Stage
      attr_reader :name

      def initialize(name, options={})
        @name = name
        @source = options.fetch(:source)
        @pipeline_stage = options.fetch(:pipeline_stage)
        @filter_strategy = options[:filter_strategy] ||
          lambda { |dataset, etl_batch| @source.filter_to_etl_batch(etl_batch)}
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

        modified_source = reextract_and_filter_source(@source, etl_batch, reextract)
        pipeline_stage.execute(modified_source)
      end

      # Returns the pipeline for this stage.
      def pipeline_stage
        @pipeline_stage.sink(:default).
          set_constant_values(:_inserted_at => Time.now)
        @pipeline_stage
      end
    end
  end
end
