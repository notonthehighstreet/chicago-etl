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
        @source = options.fetch(:source)
        @sinks = options.fetch(:sinks)
        @transformations = options.fetch(:transformations)
        @filter_strategy = options[:filter_strategy] || 
          lambda {|source, _| source }

        validate_arguments
      end

      def execute(etl_batch, reextract=false)
        transform_and_load filtered_source(source, etl_batch, reextract)
      end
      
      # Returns the named sink, if it exists
      def sink(name)
        @sinks[name.to_sym]
      end

      def sinks
        @sinks.values
      end
                  
      private
      
      def filtered_source(source, etl_batch, reextract=false)
        filtered_dataset = reextract ? source : 
          @filter_strategy.call(source, etl_batch)

        Chicago::Flow::DatasetSource.new(filtered_dataset)
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
        @transformation_chain ||= Chicago::Flow::TransformationChain.
          new(*@transformations)
      end

      def process_row(row)
        stream = row.delete(:_stream) || :default
        @sinks[stream] << row
      end

      def validate_arguments
        unless @source
          raise ArgumentError, "Stage #{@name} requires a source"
        end

        if @sinks.empty?
          raise ArgumentError, "Stage #{@name} requires at least one sink"
        end
      end
    end
  end
end
