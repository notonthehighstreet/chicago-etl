module Chicago
  module ETL
    module SequelExtensions
      module FilterToEtlBatch
        # Filters this Dataset to select records only applicable to
        # the current batch, based on the batch id.
        #
        # Applies filters on all tables selected & joined if they have
        # an etl_batch_id column.
        def filter_to_etl_batch(etl_batch)
          conditions = tables_with_etl_batch_column.
            map {|e| make_etl_batch_filter(e, etl_batch) }

          ds = apply_etl_batch_filters(conditions)

          if ds.opts[:compounds]
            ds.opts[:compounds].each do |compound|
              compound[1] = compound[1].filter_to_etl_batch(etl_batch)
            end
          end

          ds
        end

        private

        def tables_with_etl_batch_column
          (opts[:from] + (opts[:join] || [])).select {|e| has_etl_batch_column?(e) }
        end

        def apply_etl_batch_filters(conditions)
          conditions.any? ? filter(conditions.inject {|a,b| a | b}) : dup
        end
        
        def make_etl_batch_filter(expression, etl_batch)
          table = case expression
                  when Sequel::SQL::AliasedExpression
                    expression.aliaz
                  when Sequel::SQL::JoinClause
                    expression.table_alias || expression.table
                  else
                    expression
                  end

          {:etl_batch_id.qualify(table) => etl_batch.id}
        end

        def has_etl_batch_column?(expression)
          case expression
          when Sequel::SQL::AliasedExpression
            has_etl_batch_column?(expression.expression)
          when Symbol
            db.schema(expression).map(&:first).include?(:etl_batch_id)
          when Sequel::SQL::JoinClause
            has_etl_batch_column?(expression.table)
          else
            false
          end
        end
      end
    end
  end
end

Sequel::Dataset.send :include, Chicago::ETL::SequelExtensions::FilterToEtlBatch
