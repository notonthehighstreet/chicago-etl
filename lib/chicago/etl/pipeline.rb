module Chicago
  module ETL
    class Pipeline
      attr_reader :load_sources

      def initialize
        @load_sources = Schema::NamedElementCollection.new
      end

      def load_source(name)
        @load_sources[name]
      end

      def define_load_source(name, &block)
        @load_sources << LoadSource.new(name, &block)
      end
    end
  end
end
