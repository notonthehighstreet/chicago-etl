module Chicago
  module ETL
    # A Stage that passes source rows through a transformation chain.
    #
    # All rows are read into Ruby and then written to sinks after
    # passing through 0 or more Transformations.
    class RowTransformationStage < Stage
      # Returns the source for this stage.
      attr_reader :source
      
      def initialize(name, options={})
        super
        @source = options[:source]
        @sinks = options[:sinks]
        @transformations = options[:transformations] || []
        @filter_strategy = options[:filter_strategy] || lambda {|s, _| s }

        validate_arguments
      end
            
      # Executes this stage in the context of an ETL::Batch
      def perform_execution(etl_batch)
        transform_and_load filtered_source(etl_batch)
      end
      
      # Returns the named sink, if it exists
      def sink(name)
        @sinks[name.to_sym]
      end

      def sinks
        @sinks.values
      end
      
      # @api private
      def filtered_source(etl_batch)
        filtered_dataset = etl_batch.reextracting? ? source : 
          @filter_strategy.call(source, etl_batch)

        DatasetSource.new(filtered_dataset)
      end

      private

      def transform_and_load(source)
        sinks.each(&:open)
        pipe_rows_to_sinks_from(source)
        sinks.each(&:close)
      end

      def pipe_rows_to_sinks_from(source)
        source.each do |row|
          transformation_chain.process(row).each {|row| process_row(row) }
        end
        transformation_chain.flush.each {|row| process_row(row) }
      end

      def transformation_chain
        @transformation_chain ||= TransformationChain.new(*@transformations)
      end

      def process_row(row)
        stream = row.delete(:_stream) || :default
        @sinks[stream] << row
      end

      def validate_arguments
        if @source.nil?
          raise ArgumentError, "Stage #{@name} requires a source"
        end

        if @sinks.blank?
          raise ArgumentError, "Stage #{@name} requires at least one sink"
        end
      end
    end
  end
end
