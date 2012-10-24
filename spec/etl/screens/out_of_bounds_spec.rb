require 'spec_helper'

describe Chicago::ETL::Screens::OutOfBounds do
  let(:int_col) { 
    Chicago::Schema::Column.new(:int, :integer, :min => 0, :max => 100) 
  }

  let(:str_col) {
    Chicago::Schema::Column.new(:str, :string, :min => 2, :max => 5) 
  }

  it "applies to numeric columns when the value is lower than the minimum" do
    row, errors = described_class.new(:dimension_foo, int_col).
      call(:int => -1)
    
    errors.first[:error].should == "Out Of Bounds"
  end

  it "applies to numeric columns when the value is above the minimum" do
    row, errors = described_class.new(:dimension_foo, int_col).
      call(:int => 101)
    
    errors.first[:error].should == "Out Of Bounds"
  end

  it "applies to string columns when the number of chars is below minimum" do
    row, errors = described_class.new(:dimension_foo, str_col).
      call(:str => "a")
    
    errors.first[:error].should == "Out Of Bounds"
  end

  it "applies to string columns when the number of chars is above maximum" do
    row, errors = described_class.new(:dimension_foo, str_col).
      call(:str => "abcdef")
    
    errors.first[:error].should == "Out Of Bounds"
  end

  it "does not apply to string values in range" do
    row, errors = described_class.new(:dimension_foo, str_col).
      call(:str => "abcde")
    
    errors.should be_empty
  end

  it "does not apply to numeric values in range" do
    row, errors = described_class.new(:dimension_foo, int_col).
      call(:int => 0)
    
    errors.should be_empty
  end

  it "has severity 2" do
    described_class.new(:dimension_foo, int_col).severity.should == 2
  end

  it "does not replace values with default" do
    row, errors = described_class.new(:dimension_foo, str_col).
      call(:str => "a")

    row.should == {:str => "a"}
  end
end
