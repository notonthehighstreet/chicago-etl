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
    row = described_class.new(:column => string_col).process_row({})
                              
    error = row[:_errors].first
    error[:column].should == "str"
    error[:error].should == "Missing Value"
    error[:severity].should == 2
  end

  it "reports an empty string value in an expected column as a missing value" do
    row = described_class.
      new(:column => string_col).
      process_row({:str => "  "})
    
    row[:_errors].should_not be_nil
  end

  it "does not report 0 as a missing value" do
    row = described_class.new(:column => int_col).
      process_row({:int => 0})
    row[:_errors].should be_nil
  end

  it "reports missing values with severity 1 if the column is descriptive" do
    row = described_class.new(:column => descriptive_col).process_row({})
    row[:_errors].last[:severity].should == 1
  end

  it "does not report boolean values as missing" do
    row = described_class.new(:column => bool_col).process_row({})
    row[:_errors].should be_nil
  end

  it "does not report optional columns as missing values" do
    row = described_class.new(:column => optional_col).process_row({})
    row[:_errors].should be_nil
  end

  it "fills in a default value for missing values" do
    row = described_class.new(:column => optional_col).process_row({})
    row[:str].should == ''
  end
end
