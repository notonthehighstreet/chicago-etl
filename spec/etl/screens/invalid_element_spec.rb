require 'spec_helper'

describe Chicago::ETL::Screens::InvalidElement do
  let(:enum_col) { 
    Chicago::Schema::Column.new(:enum, :string, :elements => ["Foo", "Unknown"], :default => "Unknown", :optional => true) 
  }

  let(:transformation) {
    described_class.new(:table_name => :dimension_foo, :column => enum_col)
  }

  it "has a severity of 3" do
    transformation.severity.should == 3
  end

  it "reports invalid element for enum columns" do
    rows = transformation.process_row({:enum => "Bar"})
    
    rows.should include(:enum => 'Unknown')
    rows.any? {|r| r[:error] == "Invalid Element" && r[:_stream] == :error }.
      should be_true
  end

  it "does not report a valid element" do
    transformation.process_row({:enum => "foo"}).should == [{:enum => 'foo'}]
  end
end
