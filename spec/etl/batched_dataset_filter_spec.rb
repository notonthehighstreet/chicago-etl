require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::BatchedDatasetFilter do
  before :each do
    @db = stub(:db)
    @db.stub(:schema).with(:foo).and_return([[:id, {}], [:etl_batch_id, {}]])
    @db.stub(:schema).with(:bar).and_return([[:id, {}], [:etl_batch_id, {}]])
    @db.stub(:schema).with(:baz).and_return([[:id, {}]])
  end

  it "should be filterable if the table has an etl_batch_id column" do
    described_class.new(@db).should be_filterable(:foo)
    described_class.new(@db).should_not be_filterable(:baz)
  end

  it "filters a dataset by etl batch ids" do
    dataset = TEST_DB[:foo].join(:bar, :id => :id).join(:baz, :id => :id)
    
    described_class.new(@db).filter(dataset, 42).opts[:where].
      should == ({:etl_batch_id.qualify(:foo) => 42} |
                 {:etl_batch_id.qualify(:bar) => 42})
  end
end
