require 'spec_helper'

describe Chicago::ETL::DatasetSource do
  let(:dataset) { double(:dataset) }

  it "should delegtate each to the dataset" do
    dataset.should_receive(:each)
    described_class.new(dataset).each {|row| }
  end

  it "gets columns from the dataset" do
    dataset.should_receive(:columns)
    described_class.new(dataset).fields
  end
end
