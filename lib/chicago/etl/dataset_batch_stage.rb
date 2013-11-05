module Chicago
  module ETL
    # Links a PipelineStage to a Dataset.
    #
    # Allows deferring constructing a DatasetSource until extract
    # time, so that it can be filtered to an ETL batch appropriately.
    class DatasetBatchStage < Stage
      attr_reader :name

      def initialize(name, options={})
        super
        @filter_strategy = options[:filter_strategy] ||
          lambda { |dataset, etl_batch| @source.filter_to_etl_batch(etl_batch)}
        @truncate_pre_load = !!options[:truncate_pre_load]
     end

      # Executes this ETL stage.
      #
      # Configures the dataset and flows rows into the pipeline.
      def execute(etl_batch, reextract=false)
        if @truncate_pre_load
          sinks.each {|sink| sink.truncate }
        elsif reextract && sink(:error)
          sink(:error).truncate
        end
        
        sink(:default).set_constant_values(:_inserted_at => Time.now)
        super
      end
    end
  end
end
