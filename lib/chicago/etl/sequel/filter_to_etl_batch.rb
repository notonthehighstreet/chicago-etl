module Chicago
  module ETL
    module SequelExtensions
      module FilterToEtlBatch
        def filter_to_etl_batch(etl_batch)
          conditions = (opts[:from] + (opts[:join] || [])).
            select {|e| has_etl_batch_column?(e) }.
            map {|e| make_etl_batch_filter(e, etl_batch) }

          ds = conditions.any? ? filter(conditions.inject {|a,b| a | b}) : dup

          if ds.opts[:compounds]
            ds.opts[:compounds].each do |compound|
              compound[1] = compound[1].filter_to_etl_batch(etl_batch)
            end
          end

          ds
        end

        private

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
