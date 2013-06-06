require 'digest/md5'
require 'chicago/etl/buffering_insert_writer'

module Chicago
  module ETL
    # Builds a surrogate key for a dimension record, without relying
    # on the database's AUTO_INCREMENT functionality.
    #
    # We avoid AUTO_INCREMENT because we need to be able to get the
    # key mappings without having anything to do with the database -
    # this allows us to use bulk load.
    #
    # @api public
    class KeyBuilder
      # @api private
      class Factory
        attr_reader :table, :staging_db

        def initialize(table, staging_db)
          @table = table
          @staging_db = staging_db
        end

        def make
          if dimension?
            key_table = staging_db[table.key_table_name]
            sink = SchemaTableSinkFactory.new(@staging_db, table).key_sink
            
            if table.identifiable?
              IdentifiableDimensionKeyBuilder.new(key_table, sink)
            elsif existing_hash_column?(table)
              ExistingHashColumnKeyBuilder.new(key_table, sink)
            else
              HashingKeyBuilder.new(key_table, sink, columns_to_hash)
            end
          elsif fact?
            FactKeyBuilder.new(staging_db[table.table_name])
          end
        end

        private

        def existing_hash_column?(table)
          table.columns.any? {|c| c.binary? && c.name == :hash && c.unique? }
        end

        def dimension?
          table.kind_of?(Chicago::Schema::Dimension)
        end

        def fact?
          table.kind_of?(Chicago::Schema::Fact)
        end

        def columns_to_hash
          if table.natural_key.nil?
            table.columns.map(&:name)
          else
            table.natural_key
          end
        end
      end

      # Returns an appropriate key builder for a schema table, using
      # the staging database for key management where necessary.
      def self.for_table(table, staging_db)
        Factory.new(table, staging_db).make
      end

      def initialize(key_table, key_sink)
        @key_table = key_table
        @new_keys = key_sink
        @counter = Counter.new { key_table.max(:dimension_id) }
      end

      # Returns a surrogate key, given a record row.
      #
      # @raises Chicago::ETL::KeyError if the surrogate key cannot be
      #   determined from the row data.
      def key(row)
        fetch_cache unless @key_mapping
        row_id = original_key(row)
        new_key = @key_mapping[row_id]
        
        if new_key
          new_key
        else
          new_key = @counter.next
          @new_keys << {
            :original_id => key_for_insert(row_id), 
            :dimension_id => new_key
          }
          @key_mapping[row_id] = new_key
        end
      end

      # Returns the original key for the row.
      #
      # Overridden by subclasses.
      def original_key(row)
      end

      # Flushes any newly created keys to the key table.
      def close
        @new_keys.close
      end
      
      protected

      attr_reader :key_table

      def fetch_cache
        @key_mapping = key_table.
          select_hash(original_key_select_fragment, :dimension_id)
      end
    end

    # Key builder for identifiable dimensions.
    #
    # This should not be instantiated directly, use
    # KeyBuilder.for_dimension.
    #
    # @api private
    class IdentifiableDimensionKeyBuilder < KeyBuilder
      def key(row)
        raise KeyError.new("Row does not have an original_id field") unless row.has_key?(:original_id)
        super
      end

      def original_key(row)
        row[:original_id]
      end

      def key_for_insert(original_id)
        original_id
      end

      def original_key_select_fragment
        :original_id
      end
    end

    # Key builder for dimensions with a single unique hash column
    # already present.
    #
    # @api private
    class ExistingHashColumnKeyBuilder < KeyBuilder
      def original_key(row)
        row[:hash].upcase
      end

      def key_for_insert(original_id)
        ("0x" + original_id).lit
      end

      def original_key_select_fragment
        :hex.sql_function(:original_id).as(:original_id)
      end
    end

    # Key builder for dimensions with natuaral keys, but no simple
    # key.
    #
    # This should not be instantiated directly, use
    # KeyBuilder.for_dimension.
    #
    # @api private
    class HashingKeyBuilder < KeyBuilder
      attr_reader   :columns
      attr_accessor :hash_preparation

      def initialize(key_table, key_sink, columns)
        super(key_table, key_sink)
        @columns = columns
        @hash_preparation = lambda {|column| column.to_s.upcase }
      end

      def original_key(row)
        str = columns.map {|column| hash_preparation.call(row[column]) }.join
        Digest::MD5.hexdigest(str).upcase
      end

      def key_for_insert(original_id)
        ("0x" + original_id).lit
      end

      def original_key_select_fragment
        :hex.sql_function(:original_id).as(:original_id)
      end
    end

    # Returns ids for Fact tables.
    #
    # Fact table surrogate ids are transient - there is no expectation
    # that the same fact row will have the same id between
    # invocations. This is ok, because all facts should have a natural
    # key defined - the id generated by this is purely for convenience
    # and linking to error rows.
    #
    # As a result fact keys aren't stored in a key table - they are
    # never referenced by any other tables in the system.
    #
    # In addition, the same row passed twice will get a different id. 
    class FactKeyBuilder
      def initialize(db_table, key_sink=nil)
        @db_table = db_table
        @counter = Counter.new { @db_table.max(:id) }
      end

      # Returns an id given a row - the row actually has no bearing on
      # the id returned.
      def key(row)
        @counter.next
      end

      # No-op, provided for interface compatability.
      def close
      end
    end
  end
end
