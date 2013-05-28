require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'csv'

describe MysqlFileSink do
  let(:csv) { mock(:csv) }

  before :each do
    CSV.stub(:open).and_return(csv)
    csv.stub(:<<)
    csv.stub(:close).and_return(csv)
  end
  
  it "writes specified columns to rows in a file" do
    sink = described_class.new(:test_table, "test_file", [:foo])
    csv.should_receive(:<<).with([1])
    sink << {:foo => 1, :bar => 2}
  end

  it "serializes values before writing to the file" do
    sink = described_class.new(:test_table, "test_file", [:foo])
    MysqlFileSerializer.any_instance.should_receive(:serialize).with(1).and_return(1)
    sink << {:foo => 1}
  end

  it "has defined fields" do
    sink = described_class.new(:test_table, "test_file", [:foo])
    sink.should have_defined_fields
    sink.fields.should == [:foo]
  end
end
