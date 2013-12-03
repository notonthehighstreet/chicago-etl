module Chicago
  module ETL
    class SchemaTableSinkBuilder < StageBuilder::SinkBuilder
      def initialize(db, schema_table, key_mappings)
        @db = db
        @schema_table = schema_table
        @sink_factory = SchemaTableSinkFactory.new(@db, @schema_table)
        @key_mappings = key_mappings
      end

      def build(&block)
        @sinks = {}
        
        add default_sink, :stream => :default
        add key_sink, :stream => :dimension_key
        add error_sink, :stream => :error

        @key_mappings.each do |m|
          add @sink_factory.key_sink(:table => m.table), :stream => m.table
        end

        instance_eval(&block)
        @sinks
      end

      private

      def default_sink
        @sink_factory.sink
      end

      def error_sink
        @sink_factory.error_sink
      end

      def key_sink
        if @schema_table.kind_of?(Chicago::Schema::Dimension)
          @sink_factory.key_sink
        else
          # Facts have no key table to write to.
          NullSink.new
        end
      end
    end
  end
end
