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

          row[@column_name] = UkPostCodeField.new.call(row[@column_name])[:post_code]

          [row, errors]
        end
      end
    end
  end
end
