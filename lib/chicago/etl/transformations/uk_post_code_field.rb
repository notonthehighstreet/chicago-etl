# -*- coding: utf-8 -*-

module Chicago
  module ETL
    module Transformations
      class UkPostCodeField
        MATCH = /\A([A-Z][A-Z]?[0-9][0-9A-Z]?)(?:([0-9][A-Z]{2}))?\Z/
        
        # Returns cleaned, formatted data about a UK Post Code.
        #
        # Example:
        #
        #     UkPostCodeField.new.normalize(" SW !2 4 GH")
        #     # => { :post_code => "SW12 4GH",
        #            :outcode => "SW12",
        #            :incode => "4GH" }
        #
        # Partial postcodes will be returned without the incode. BFPO
        # postcodes are supported, but have no incode or
        # outcode. Postcodes that do not follow the format will be
        # returned as is, with an invalid key set.
        def normalize(raw_post_code)
          reformat(clean(raw_post_code)) ||
            {:post_code => raw_post_code, :invalid => true}
        end

        private

        def clean(raw_post_code)
          raw_post_code.
            strip.
            upcase.
            tr('!"$%^&*()', '124567890').
            gsub("£", "3").
            sub(/^0([XL])/, 'O\1').
            sub(/^([PSCY])0/, '\1O')
        end

        def reformat(post_code)
          if post_code[0..3] == "BFPO"
            { :post_code => post_code.sub(/BFPO\s*/, "BFPO ") }
          else
            reformat_standard_post_code(post_code)
          end
        end
        
        def reformat_standard_post_code(post_code)
          match = post_code.gsub(/\s+/,'').match(MATCH)

          unless match.nil?
            { :outcode => match[1],
              :incode => match[2],
              :post_code => [match[1], match[2]].join(' ').strip }
          end
        end
      end
    end
  end
end
