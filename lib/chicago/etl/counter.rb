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

      # Creates a new counter, optionally with a starting count.
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
