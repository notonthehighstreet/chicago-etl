require 'set'

module Chicago
  module ETL
    class LoadDatasetBuilder
      def initialize
        @renames = {}
        @joins = []
      end

      def configure(&block)
        instance_eval(&block)
        self
      end

      def table(table_name)
        @table_name = table_name
        self
      end

      def denormalize(table, keys)
        @joins << [:left_outer, table, keys]
      end

      def rename(source_name, target_name)
        @renames[target_name] = source_name
      end

      def build(db, columns)
        dataset = @joins.inject(db[@table_name]) {|ds, join|
          ds.join_table(*join)
        }

        available_columns = dataset.dependant_tables.inject({}) do |hsh, table|
          hsh[table] = Set.new(db[table].columns)
          hsh
        end

        select_columns = columns.map {|name|
          if @renames[name]
            @renames[name].as(name)
          else
            if available_columns[@table_name].include?(name)
              name
            else
              
          end
        }

        dataset.select(*select_columns)
      end
    end
  end
end
