require 'spec_helper'

describe Chicago::ETL::MysqlDumpfile do
  before :each do
    @csv = mock(:csv)
  end
  
  it "outputs specified column values in order" do
    dumpfile = described_class.new(@csv, [:foo, :bar])
    @csv.should_receive(:<<).with(["1", "2"])

    dumpfile << {:foo => "1", :bar => "2", :baz => "not output"}
  end

  it "transforms values with a MysqlLoadFileValueTransformer" do
    transformer = mock(:transformer)
    Chicago::ETL::MysqlLoadFileValueTransformer.stub(:new).and_return(transformer)

    transformer.should_receive(:transform).with("bar").and_return("baz")
    @csv.should_receive(:<<).with(["baz"])

    dumpfile = described_class.new(@csv, [:foo])
    dumpfile << {:foo => "bar"}
  end

  it "will write a row only once with the same key" do
    dumpfile = described_class.new(@csv, [:foo], :id)
    @csv.should_receive(:<<).with(["bar"])
    
    dumpfile << {:id => 1, :foo => "bar"}
    dumpfile << {:id => 1, :foo => "baz"}
  end

  it "will write a row multiple times if no key is specified" do
    dumpfile = described_class.new(@csv, [:foo])
    @csv.should_receive(:<<).with(["bar"])
    @csv.should_receive(:<<).with(["baz"])
    
    dumpfile << {:id => 1, :foo => "bar"}
    dumpfile << {:id => 1, :foo => "baz"}
  end
end