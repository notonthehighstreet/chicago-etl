require 'spec_helper'

describe Chicago::ETL::Transformations::AddBatchId do
  subject { described_class.new(1) }

  it "should add the batch id to the row" do
    subject.call([:errors], {}).should == [[:errors], [{:etl_batch_id => 1}]]
  end
end
