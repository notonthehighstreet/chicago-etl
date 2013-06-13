require 'spec_helper'

describe Chicago::ETL::Screens::OutOfBounds do
  let(:int_col) { 
    Chicago::Schema::Column.new(:int, :integer, :min => 0, :max => 100) 
  }

  let(:str_col) {
    Chicago::Schema::Column.new(:str, :string, :min => 2, :max => 5) 
  }

  let(:int_transformation) {
    described_class.new(:table_name => :dimension_foo, :column => int_col)
  }

  let(:str_transformation) {
    described_class.new(:table_name => :dimension_foo, :column => str_col)
  }

  it "applies to numeric columns when the value is lower than the minimum" do
    row = int_transformation.process_row(:int => -1)
    row[:_errors].first[:error].should == "Out Of Bounds"
  end

  it "applies to numeric columns when the value is above the minimum" do
    row = int_transformation.process_row(:int => 101)
    row[:_errors].first[:error].should == "Out Of Bounds"
  end

  it "applies to string columns when the number of chars is below minimum" do
    row = str_transformation.process_row(:str => "a")
    row[:_errors].first[:error].should == "Out Of Bounds"
  end

  it "applies to string columns when the number of chars is above maximum" do
    row = str_transformation.process_row(:str => "abcdef")
    row[:_errors].first[:error].should == "Out Of Bounds"
  end

  it "does not apply to string values in range" do
    str_transformation.process_row(:str => "abcde").
      should_not have_key(:_errors)
  end

  it "does not apply to numeric values in range" do
    int_transformation.process_row(:int => 0).should_not have_key(:_errors)
  end

  it "has severity 2" do
    int_transformation.severity.should == 2
  end

  it "does not replace values with default" do
    str_transformation.process_row(:str => "a")[:str].should == "a"
  end
end
