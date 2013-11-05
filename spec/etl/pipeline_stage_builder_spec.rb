require 'spec_helper'

describe Chicago::ETL::SchemaSinksAndTransformationsBuilder do
  let(:dimension) { stub(:dimension).as_null_object }
  let(:db) { stub(:db).as_null_object }
  let(:sink_factory) { stub(:sink_factory).as_null_object }

  before(:each) {
    Chicago::ETL::SchemaTableSinkFactory.stub(:new).and_return(sink_factory)
  }

  it "should exclude columns from the sink" do
    sink_factory.should_receive(:sink).
      with(:ignore => false, :exclude => [:foo]).
      and_return(stub(:sink).as_null_object)

    described_class.new(db, dimension).build do
      load_separately :foo
    end
  end

  it "can specify rows are not going to be replaced" do
    sink_factory.should_receive(:sink).
      with(:ignore => true, :exclude => []).
      and_return(stub(:sink).as_null_object)

    described_class.new(db, dimension).build do
      ignore_present_rows
    end
  end

  it "can add key mappings" do
    stage = described_class.new(db, dimension).build do
      key_mapping :bar, :original_id
    end

    stage[:sinks][:bar].should_not be_nil
  end
end
