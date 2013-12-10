module Chicago
  module ETL
    # @api private
    class TransformationChain
      def initialize(*transforms)
        @transforms = transforms
      end

      def output_streams
        @transforms.inject([]) {|s, t| s | t.output_streams }
      end

      def process(row)
        @transforms.inject([row]) do |rows, transform|
          process_rows(rows, transform)
        end
      end

      def flush
        @transforms.inject([]) do |rows, transform|
          process_rows(rows, transform) + transform.flush
        end
      end

      def upstream_fields(fields)
        @transforms.inject(fields) {|t| t.upstream_fields(fields) }
      end

      def downstream_fields(fields)
        @transforms.inject(fields) {|t| t.downstream_fields(fields) }
      end

      private

      def process_rows(rows, transform)
        rows.inject([]) {|all, row| all.concat(transform.process(row)) }
      end
    end
  end
end
