module Chicago
  module ETL
    # Provides convenience methods for defining source datasets.
    class DatasetBuilder
      attr_reader :db

      # @api private
      def initialize(db)
        @db = db
      end

      # @api private
      def build(&block)
        instance_eval(&block)
      end

      protected
      
      def key_field(field, name)
        :if[{field => nil}, 1, field].as(name)
      end
      
      # Returns a column for use in a Sequel::Dataset#select method to
      # return a dimension key.
      #
      # Takes care of using the key tables correctly, and dealing with
      # missing dimension values.
      def dimension_key(name)
        key_field("keys_dimension_#{name}__dimension_id".to_sym,
                  "#{name}_dimension_id".to_sym)
      end

      # Returns a column for use in a Sequel::Dataset#select method to
      # return a date dimension key.
      def date_dimension_column(dimension)
        :if.sql_function({:id.qualify(dimension) => nil},
                         1, 
                         :id.qualify(dimension)).
          as("#{dimension}_dimension_id".to_sym)
      end

      # Rounds a monetary value to 2 decimal places.
      #
      # By default, natural rounding is used, you can specify either
      # :up or :down as the direction.
      #
      # @deprecated
      def round(stmt, direction = :none)
        case direction
        when :none
          :round.sql_function(stmt, 2)
        when :up
          :ceil.sql_function(stmt * 100) / 100
        when :down
          :floor.sql_function(stmt * 100) / 100
        end
      end
    end
  end
end
