require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'sequel'

describe MysqlFileSink do
  let(:dataset) { mock(:dataset).as_null_object }
  let(:db) { mock(:db, :[] => dataset, :schema => []) }
  let(:csv) { mock(:csv) }

  let(:sink) {
    described_class.new(db, :table, "test_file", [:foo])
  }
  
  before :each do
    CSV.stub(:open).and_return(csv)
    csv.stub(:<<)
    csv.stub(:close).and_return(csv)
  end
  
  it "writes specified columns to rows in a file" do
    csv.should_receive(:<<).with([1])
    sink << {:foo => 1, :bar => 2}
  end

  it "serializes values before writing to the file" do
    MysqlFileSerializer.any_instance.should_receive(:serialize).with(1).and_return(1)
    sink << {:foo => 1}
  end

  it "has defined fields" do
    sink.should have_defined_fields
    sink.fields.should == [:foo]
  end

  it "loads the csv file into the database when closed" do
    dataset.should_receive(:load_csv_infile).with("test_file", [:foo])
    sink.close
  end

  it "specifies that INSERT IGNORE should be used" do
    dataset.should_receive(:insert_ignore)
    sink.close
  end
end
