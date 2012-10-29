# -*- coding: utf-8 -*-
module Chicago
  module ETL
    module Transformations
      # Cleans and reformats UK-based postcodes.
      #
      # Transformations are based on observed errors in data entry, so
      # shift key slips (i.e. typing '!' where '1' was meant) are
      # corrected, as are use of numbers where letters were intended
      # i.e. (0X -> OX for Oxfordshire postcodes).
      #
      # Leaves BFPO postcodes alone.
      class UkPostCode
        # Creates a new post code transformation.
        #
        # @param Symbol column_name - the name of the column
        #   containing the post code.
        #
        # @param Proc filter_block - an optional block, which takes a
        #   row. If the block returns false, the transformation will
        #   not be run. This can be useful to only run the
        #   transformation on UK addresses, based a country field in
        #   the row for example.
        def initialize(column_name, &filter_block)
          @column_name = column_name
          @filter_block = filter_block
        end

        def call(row, errors=[])
          return [row, errors] if @filter_block && !@filter_block.call(row)

          clean_post_code = row[@column_name].
            strip.
            upcase.
            tr('!"$%^&*()', '124567890').
            gsub("£", "3").
            sub(/^0([XL])/, 'O\1').
            sub(/^([PSCY])0/, '\1O')

          row[@column_name] = reformat(clean_post_code)

          [row, errors]
        end

        private

        def reformat(post_code)
          unless post_code[0..3] == "BFPO"
            post_code.gsub!(/[^A-Z0-9]/, '')
            post_code = "#{post_code[0..(post_code.size - 4)]} #{post_code[(post_code.size - 3)..-1]}" if post_code.size > 4
          end

          post_code
        end
      end
    end
  end
end
