module Chicago
  module ETL
    module Screens
      class CompositeScreen
        def initialize(*screens)
          @screens = screens.flatten
        end

        def call(row, errors=[])
          @screens.inject([row,errors]) do |(row, errors), screen| 
            screen.call(row, errors) 
          end
        end
      end
    end
  end
end
