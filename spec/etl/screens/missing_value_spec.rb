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
    row, errors = described_class.new(:dimension_foo, string_col).call({})
    
    errors.first[:table].should == "dimension_foo"
    errors.first[:column].should == "str"
    errors.first[:error].should == "Missing Value"
    errors.first[:severity].should == 2
  end

  it "reports an empty string value in an expected column as a missing value" do
    row, errors = described_class.new(:dimension_foo, string_col).
      call({:str => "  "})
    
    errors.first[:error].should == "Missing Value"
  end

  it "does not report 0 as a missing value" do
    row, errors = described_class.new(:dimension_foo, int_col).
      call({:int => 0})
    
    errors.should be_empty
  end

  it "reports missing values with severity 1 if the column is descriptive" do
    row, errors = described_class.new(:dimension_foo, descriptive_col).call({})
    errors.first[:severity].should == 1
  end

  it "does not report boolean values as missing" do
    row, errors = described_class.new(:dimension_foo, bool_col).call({})
    errors.should be_empty
  end

  it "does not report optional columns as missing values" do
    row, errors = described_class.new(:dimension_foo, optional_col).call({})
    errors.should be_empty
  end

  it "fills in a default value for missing values" do
    row, errors = described_class.new(:dimension_foo, optional_col).call({})
    row.should == {:str => ''}
  end
end
