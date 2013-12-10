module Chicago
  module ETL
    # Provides DSL methods for specifying the pipeline in an ETL
    # stage.
    #
    # Clients will not normally instantiate this themselves but use it
    # in the context of defining an ETL stage.
    class SchemaTableTransformationBuilder
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
      def initialize(db, schema_table, load_separately, key_mappings)
        @db = db
        @schema_table = schema_table
        @load_separately = load_separately || []
        @key_mappings = key_mappings || []
      end

      # @api private
      def build(&block)
        @transformations = {}
        TRANSFORMATION_ORDER.each {|k| @transformations[k] = [] }

        instance_eval &block

        add_screens
        add_key_transforms
        add_final_transforms

        TRANSFORMATION_ORDER.map {|k| @transformations[k] }.flatten
      end

      protected

      # Add a transformation at a specific point in the transformation
      # chain (defined in TRANSFORMATION_ORDER)
      def add(transform, position)
        location, point = position.to_a.first
        key = "#{location}_#{point}".to_sym
        @transformations[key] << transform
      end

      private

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
