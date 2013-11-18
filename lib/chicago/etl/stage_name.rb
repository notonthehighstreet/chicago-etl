module Chicago
  module ETL
    # A namespaced name for an ETL stage.
    #
    # @api private
    class StageName
      def initialize(*names)
        if names.size == 1 && names.first.kind_of?(String)
          @names = names.first.split(".").map(&:to_sym).freeze
        else
          @names = names.flatten.map(&:to_sym).freeze
        end
      end

      def name
        @names.last
      end

      def match?(*pattern)
        pattern.flatten!
        return false if pattern.size > @names.size

        pattern.each_with_index.all? do |part, i|
          part == :* || @names[i] == part
        end
      end
      alias :=~ :match?

      def namespace
        @names[0...(@names.size - 1)]
      end

      def eql?(other)
        to_s == other.to_s
      end
      alias :== :eql?

      def hash
        to_s.hash
      end

      def to_a
        @names.dup
      end

      def to_s
        @string_representation ||= @names.join('.')
      end
    end
  end
end
