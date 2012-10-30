module Chicago
  module ETL
    module Transformations
      class AddInsertTimestamp
        def initialize(timestamp=Time.now)
          @insert_timestamp = timestamp.utc
        end

        def call(row, errors=[])
          row[:_inserted_at] = @insert_timestamp
          [row, errors]
        end
      end
    end
  end
end
