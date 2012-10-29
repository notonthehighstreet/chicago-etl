require 'spec_helper'

describe Chicago::ETL::Screens::CompositeScreen do
  let(:screen) do
    i = 0

    lambda {|row, errors| 
      i += 1 
      errors << i 
      [row, errors]
    }
  end

  it "calls all child screens" do
    row, errors = described_class.new([screen, screen]).call({:a => 1}, [])
    row.should == {:a => 1}
    errors.should == [1,2]
  end

  it "supports variable arguments in the constructor" do
    row, errors = described_class.new(screen, screen).call({:a => 1}, [])
    row.should == {:a => 1}
    errors.should == [1,2]
  end
end
