module Chicago
  module ETL
    # @api public
    class Filter < Transformation
      def initialize(stream=:default, &block)
        super(stream)
        @block = block || lambda {|row| false }
      end

      def process_row(row)
        row if @block.call(row)
      end
    end
  end
end
