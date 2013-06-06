require 'spec_helper'

describe Chicago::ETL::Screens::MissingValue do
  let(:string_col) { Chicago::Schema::Column.new(:str, :string) }
  let(:int_col)    { Chicago::Schema::Column.new(:int, :integer) }
  let(:bool_col)   { Chicago::Schema::Column.new(:bool, :boolean) }
  
  let(:descriptive_col) { 
    Chicago::Schema::Column.new(:str, :string, :descriptive => true) 
  }

  let(:optional_col) { 
    Chicago::Schema::Column.new(:str, :string, :optional => true) 
  }

  it "reports nil in an expected column as a missing value, with severity 2" do
    rows = described_class.new(:table_name => :dimension_foo, 
                               :column => string_col).process_row({})
    rows.last[:table].should == "dimension_foo"
    rows.last[:column].should == "str"
    rows.last[:error].should == "Missing Value"
    rows.last[:severity].should == 2
  end

  it "reports an empty string value in an expected column as a missing value" do
    rows = described_class.new(:table_name => :dimension_foo, 
                               :column => string_col).
      process_row({:str => "  "})
    
    rows.last[:error].should == "Missing Value"
  end

  it "does not report 0 as a missing value" do
    rows = described_class.new(:table_name => :dimension_foo, :column => int_col).
      process_row({:int => 0})
    rows.size.should == 1
  end

  it "reports missing values with severity 1 if the column is descriptive" do
    rows = described_class.new(:table_name => :dimension_foo, :column => descriptive_col).process_row({})
    rows.last[:severity].should == 1
  end

  it "does not report boolean values as missing" do
    rows = described_class.new(:table_name => :dimension_foo, :column => bool_col).process_row({})
    rows.size.should == 1
  end

  it "does not report optional columns as missing values" do
    rows = described_class.new(:table_name => :dimension_foo, :column => optional_col).process_row({})
    rows.size.should == 1
  end

  it "fills in a default value for missing values" do
    rows = described_class.new(:table_name => :dimension_foo, :column => optional_col).process_row({})
    rows.should == [{:str => ''}]
  end
end
