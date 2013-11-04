module Chicago
  module ETL
    class Stage
      attr_reader :name

      def initialize(name, options={})
        @name = name
        @source = options.fetch(:source)
        raise ArgumentError, "Stage #{name} requires a source" unless @source

        @sinks = options.fetch(:sinks)
        raise ArgumentError, "Stage #{name} requires at least one sink" if @sinks.empty?

        @transformations = options.fetch(:transformations)
        @transformation_chain = Chicago::Flow::TransformationChain.
          new(*@transformations)

        @filter_strategy = options[:filter_strategy] || 
          lambda {|source, _| source }
      end

      def execute(etl_batch, reextract)
        modified_source = reextract_and_filter_source(@source, etl_batch, reextract)
        transform_and_load_from(modified_source)
      end
      
      def transform_and_load_from(source)
      end

      def reextract_and_filter_source(source, etl_batch, reextract=false)
        if reextract
          filtered_dataset = source
        else
          filtered_dataset = @filter_strategy.call(source, etl_batch)
        end
        Chicago::Flow::DatasetSource.new(filtered_dataset)
      end

          attr_reader :transformation_chain
      
      # Returns the named sink, if it exists
      def sink(name)
        @sinks[name.to_sym]
      end

      def sinks
        @sinks.values
      end

      def register_sink(name, sink)
        @sinks[name.to_sym] = sink
        self
      end
            
      def transform_and_load_from(source)
        sinks.each(&:open)
        pipe_rows_to_sinks_from(source)
        sinks.each(&:close)
      end
      
      private
      
      def pipe_rows_to_sinks_from(source)
        source.each do |row|
          transformation_chain.process(row).each {|row| process_row(row) }
        end
        transformation_chain.flush.each {|row| process_row(row) }
      end

      def process_row(row)
        stream = row.delete(:_stream) || :default
        @sinks[stream] << row
      end
    end
  end
end
