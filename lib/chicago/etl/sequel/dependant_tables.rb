require 'sequel'

module Chicago
  module ETL
    module SequelExtensions
      module DependantTables
        # Returns an Array of table names used in this dataset.
        #
        # Handles joins and if the recurse flag is true, unions and
        # nested datasets.
        def dependant_tables(recurse=true)
          tables = extract_dependant_tables_in_clause(opts[:from].first, recurse)

          if opts[:compounds]
            tables += opts[:compounds].map {|(_, dataset, _)|
              dataset.dependant_tables
            }
          end

          if opts[:join]
            tables += opts[:join].map {|join| 
              extract_dependant_tables_in_clause(join.table, recurse)
            }
          end

          tables.flatten.uniq
        end

        private
        
        def extract_dependant_tables_in_clause(clause, recurse)
          case clause
          when Symbol
            [clause]
          when Sequel::SQL::AliasedExpression
            extract_dependant_tables_in_clause(clause.expression, recurse)
          when Sequel::Dataset
            recurse ? clause.dependant_tables : []
          else
            []
          end
        end
      end
    end
  end
end

Sequel::Dataset.send :include, Chicago::ETL::SequelExtensions::DependantTables
