require 'spec_helper'

describe Chicago::ETL::Screens::InvalidElement do
  let(:enum_col) { 
    Chicago::Schema::Column.new(:enum, :string, :elements => ["Foo", "Unknown"], :default => "Unknown", :optional => true) 
  }

  it "has a severity of 3" do
    described_class.new(:dimension_foo, enum_col).severity.should == 3
  end

  it "reports invalid element for enum columns" do
    row, errors = described_class.new(:dimension_foo, enum_col).
      call({:enum => "Bar"})
    row.should == {:enum => 'Unknown'}

    errors.first[:error].should == "Invalid Element"
  end

  it "does not report a valid element" do
    row, errors = described_class.new(:dimension_foo, enum_col).
      call({:enum => "foo"})
    row.should == {:enum => 'foo'}

    errors.should be_empty
  end
end
