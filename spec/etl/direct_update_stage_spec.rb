require 'spec_helper'

describe Chicago::ETL::DirectUpdateStage do
  let(:etl_batch) { stub(:etl_batch, :reextracting? => true) }

  it "updates the dataset when executed" do
    dataset = mock(:dataset)
    updates = {:foo => :bar}

    dataset.should_receive(:update).with(:foo => :bar)

    described_class.new("name", :source => dataset, :updates => updates).
      execute(etl_batch)
  end
end
