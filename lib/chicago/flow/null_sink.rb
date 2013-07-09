module Chicago
  module Flow
    # Supports the Sink interface, but discards all rows written to
    # it.
    class NullSink < Sink
    end
  end
end
