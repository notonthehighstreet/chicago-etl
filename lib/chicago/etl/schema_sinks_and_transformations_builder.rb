module Chicago
  module ETL
    # Provides DSL methods for specifying the pipeline in an ETL
    # stage.
    #
    # Clients will not normally instantiate this themselves but use it
    # in the context of defining an ETL stage.
    class SchemaSinksAndTransformationsBuilder
      # @api private
      KeyMapping = Struct.new(:table, :field)

      # The ordering of inbuilt transformation and screening steps.
      TRANSFORMATION_ORDER = [:before_screens,
                              :screens,
                              :after_screens,
                              :before_keys,
                              :keys,
                              :after_keys,
                              :before_final,
                              :final,
                              :after_final
                             ].freeze

      # @api private
      def initialize(db, schema_table)
        @db = db
        @schema_table = schema_table
        @sink_factory = SchemaTableSinkFactory.new(@db, @schema_table)
      end

      # @api private
      def build(&block)
        @load_separately = []
        @key_mappings    = []
        @transformations = {}
        TRANSFORMATION_ORDER.each {|k| @transformations[k] = [] }
        @ignore_present_rows = false

        instance_eval &block

        add_screens
        add_key_transforms
        add_final_transforms
        sinks_and_transformations = create_sinks_and_transformations
        register_additional_sinks(sinks_and_transformations)
        sinks_and_transformations
      end

      protected

      # Ignore rows already present in the target table, rather than
      # replacing them.
      def ignore_present_rows
        @ignore_present_rows = true
      end

      # Specify columns that won't be loaded or screened as part of
      # this pipeline stage
      def load_separately(*columns)
        @load_separately += columns
      end

      # Add an additional key mapping.
      def key_mapping(table, field)
        @key_mappings << KeyMapping.new(table, field)
      end

      # Add a transformation before the specified point in the
      # transformation chain (defined in TRANSFORMATION_ORDER)
      def before(point_in_transformation_chain, transform)
        key = "before_#{point_in_transformation_chain}".to_sym
        @transformations[key] << transform
      end

      # Add a transformation after the specified point in the
      # transformation chain (defined in TRANSFORMATION_ORDER)
      def after(point_in_transformation_chain, transform)
        key = "after_#{point_in_transformation_chain}".to_sym
        @transformations[key] << transform
      end

      private

      def create_sinks_and_transformations
        default = @sink_factory.sink(:ignore => @ignore_present_rows,
                                     :exclude => @load_separately)
        key_sink = if @schema_table.kind_of?(Chicago::Schema::Dimension)
                     @sink_factory.key_sink
                   else
                     # Facts have no key table to write to.
                     Flow::NullSink.new
                   end

        {
          :transformations => concat_transformations,
          :sinks => {
            :default => default,
            :dimension_key => key_sink,
            :error => @sink_factory.error_sink
          }
        }
      end

      def concat_transformations
        TRANSFORMATION_ORDER.map {|k| @transformations[k] }.flatten
      end

      def register_additional_sinks(sinks_and_transformations)
        sinks = sinks_and_transformations[:sinks]
        @key_mappings.each do |mapping|
          sink = @sink_factory.key_sink(:table => mapping.table)
          sinks[mapping.table] = sink
        end
      end

      def add_screens
        columns_to_screen = @schema_table.columns.reject do |column| 
          @load_separately.include?(column.name)
        end

        @transformations[:screens] = [Screens::MissingValue,
                                      Screens::InvalidElement,
                                      Screens::OutOfBounds].map do |klass|
          klass.for_columns(columns_to_screen)
        end.flatten
      end

      def add_key_transforms
        @transformations[:keys] << Transformations::AddKey.
          new(:key_builder => KeyBuilder.for_table(@schema_table, @db))

        @key_mappings.each do |mapping|
          @transformations[:keys] << Transformations::DimensionKeyMapping.
            new(:original_key => mapping.field, :key_table => mapping.table)
        end
      end

      def add_final_transforms
        @transformations[:final] << Transformations::WrittenRowFilter.new(:key => :id)
        @transformations[:final] << Transformations::DemultiplexErrors.new
      end
    end
  end
end
