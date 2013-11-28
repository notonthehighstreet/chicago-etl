module Chicago
  module ETL
    # A Stage in the ETL pipeline.
    #
    # A Stage wires together a Source, 0 or more Transformations and 1
    # or more Sinks.
    class Stage
      # Returns the source for this stage.
      attr_reader :source
      
      # Returns the name of this stage.
      attr_reader :name

      def initialize(name, options={})
        @name = name
        @source = options[:source]
        @sinks = options[:sinks]
        @transformations = options[:transformations] || []
        @filter_strategy = options[:filter_strategy] || lambda {|s, _| s }
        @pre_execution_strategies = options[:pre_execution_strategies] || []
        @executable = options.has_key?(:executable) ? options[:executable] : true

        validate_arguments
      end
      
      # Returns the unqualified name of this stage.
      def task_name
        name.name
      end
      
      # Returns true if this stage should be executed.
      def executable?
        @executable
      end
      
      # Executes this stage in the context of an ETL::Batch
      def execute(etl_batch)
        prepare_stage(etl_batch)
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

      def prepare_stage(etl_batch)
        @pre_execution_strategies.each do |strategy| 
          strategy.call(self, etl_batch)
        end
      end

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
