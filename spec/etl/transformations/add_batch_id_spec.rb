require 'spec_helper'

describe Chicago::ETL::Transformations::AddEtlBatchId do
  it "should add the batch id to the row" do
    described_class.new(1).call({}, []).should == [{:etl_batch_id => 1}, []]
  end
end
