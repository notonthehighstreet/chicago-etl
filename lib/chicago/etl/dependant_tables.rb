require 'sequel'

module Chicago
  module SequelExtensions
    module DependantTables
      # Returns an Array of table names used in this dataset.
      #
      # Handles joins, unions and nested datasets.
      def dependant_tables
        tables = extract_dependant_tables_in_clause(opts[:from].first)

        if opts[:compounds]
          tables += opts[:compounds].map {|(_, dataset, _)|
            dataset.dependant_tables
          }
        end

        if opts[:join]
          tables += opts[:join].map {|join| 
            extract_dependant_tables_in_clause(join.table)
          }
        end

        tables.flatten.uniq
      end

      private
      
      def extract_dependant_tables_in_clause(clause)
        case clause
        when Symbol
          [clause]
        when Sequel::SQL::AliasedExpression
          extract_dependant_tables_in_clause(clause.expression)
        when Sequel::Dataset
          clause.dependant_tables
        else
          []
        end
      end
    end
  end
end

Sequel::Dataset.send :include, Chicago::SequelExtensions::DependantTables
