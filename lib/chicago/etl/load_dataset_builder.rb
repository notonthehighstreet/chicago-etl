require 'set'

module Chicago
  module ETL
    class LoadDatasetBuilder
      def initialize(&block)
        @constructed_columns = {}
        @joins = []
        configure(&block) if block_given?
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
        self
      end

      def provide(target_name, source_column)
        @constructed_columns[target_name] = source_column
        self
      end

      def build(db, columns)
        dataset = @joins.inject(db[@table_name]) {|ds, join|
          ds.join_table(*join)
        }

        available_columns = available_columns_index(dataset)

        select_columns = columns.map {|name|
          if @constructed_columns[name].kind_of?(Symbol)
            qualify_column(available_columns, @constructed_columns[name]).as(name)
          elsif @constructed_columns[name]
            @constructed_columns[name].as(name)
          else
            qualify_column(available_columns, name)
          end
        }

        dataset.select(*select_columns)
      end
      
      private

      def available_columns_index(dataset)
        dataset.dependant_tables.inject({}) do |hsh, table|
          db[table].columns.each do |column|
            (hsh[column] ||= Set.new) << table
          end
          hsh
        end
      end

      def qualify_column(available_columns, name)
        if available_columns[name] && available_columns[name].size == 1
          name.qualify(available_columns[name].first)
        elsif name == :id
          name.qualify(@table_name)
        else
          raise "Column #{name} was either ambiguous or non-existant"
        end
      end
        
    end
  end
end
