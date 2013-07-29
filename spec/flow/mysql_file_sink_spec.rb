require 'spec_helper'
require 'sequel'

describe MysqlFileSink do
  let(:dataset) { mock(:dataset).as_null_object }
  let(:db) { mock(:db, :[] => dataset, :schema => []) }
  let(:csv) { mock(:csv) }

  let(:sink) {
    described_class.new(db, :table, [:foo], :filepath => "test_file")
  }
  
  before :each do
    CSV.stub(:open).and_return(csv)
    csv.stub(:<<)
    csv.stub(:close).and_return(csv)
    csv.stub(:flush)

    File.stub(:size?).and_return(true)
  end
  
  it "has the same name as the table it is loading into" do
    sink.name.should == :table
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
    dataset.should_receive(:load_csv_infile).
      with("test_file", [:foo], :set => {})
    sink.close
  end

  it "uses the :set hash to load constant values" do
    sink.set_constant_values(:bar => 1).should == sink
    dataset.should_receive(:load_csv_infile).
      with("test_file", [:foo], :set => {:bar => 1})
    sink.close
  end

  it "does not IGNORE rows by default" do
    dataset.should_not_receive(:insert_ignore)
    sink.close
  end

  it "can specify that INSERT IGNORE should be used" do
    dataset.should_receive(:insert_ignore)
    described_class.new(db, :table, [:foo], 
                        :filepath => "test_file", :ignore => true).close
  end

  it "writes csv to a tempfile if no explicit filepath is given" do
    described_class.new(db, :table, [:foo]).filepath.should match(/table\.\d+\.csv/)
  end

  it "doesn't attempt to load data if the file is empty or does not exist" do
    File.stub(:size?).and_return(false)
    dataset.should_not_receive(:load_csv_infile)
    sink.close
  end

  it "removes the temporary file when closed" do
    File.stub(:exists?).and_return(true)
    File.should_receive(:unlink).with("test_file")

    sink.close
  end

  it "truncates the table by default" do
    dataset.should_receive(:truncate)
    sink.truncate
  end

  it "can have a truncation strategy set" do
    x = nil    
    sink.truncation_strategy = lambda { x = "deleted table" }
    sink.truncate
    x.should == "deleted table"
  end
end
