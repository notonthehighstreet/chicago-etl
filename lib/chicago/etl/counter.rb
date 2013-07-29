require 'thread'

module Chicago
  module ETL
    # Provides a thread-safe wrapper around an incrementing number.
    #
    # Intended to be used for key builders, rather than using the
    # database's AUTO INCREMENT functionality.
    #
    # @api private
    class Counter
      # Returns the current number this counter is on.
      attr_reader :current

      # Creates a new counter.
      #
      # May optionally be created with a starting count, either as a
      # number or as a block which generates a number.
      #
      #     Counter.new(41).next # returns 42
      #     Counter.new { 2 + 2 }.next # returns 5
      #     
      def initialize(current_number=0, &block)
        @mutex = Mutex.new
        if block
          @block = block
        else
          @current = current_number || 0
        end
      end

      # Returns the next number.
      #
      # Modifies the current state of the counter.
      def next
        @current = (@block.call || 0) if @current.nil?
        @mutex.synchronize do
          @current += 1
        end
      end
    end
  end
end
