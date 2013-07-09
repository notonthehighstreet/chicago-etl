module Chicago
  module Flow
    # @api public
    class Error < RuntimeError
    end
    
    # @api public
    class RaisingErrorHandler
      def unregistered_sinks(sinks)
        raise Error.new("Sinks not registered: #{sinks.join(",")}")
      end
    end
  end
end
