require 'spec_helper'

describe Chicago::ETL::Stage do
  let(:etl_batch) { double(:etl_batch, :reextracting? => true) }

  it "requires a source" do
    expect {
      described_class.new(:test,
                          :source => nil,
                          :sinks => {:default => double(:sink)})
    }.to raise_error(ArgumentError)
  end

  it "requires sinks" do
    expect {
      described_class.new(:test,
                          :source => double(:source),
                          :sinks => nil)
    }.to raise_error(ArgumentError)
  end

  it "does not filter the dataset if re-extracting" do
    stage = described_class.new(:test,
                                :source => double(:source),
                                :sinks => {:default => double(:sink)},
                                :filter_strategy => lambda { fail })

    stage.filtered_source(etl_batch)
  end

  it "opens sinks before writing and closes them afterwards" do
    sink = double(:sink)
    sink.should_receive(:open)
    sink.should_receive(:close)

    stage = described_class.new(:test,
                                :source => [],
                                :sinks => {:default => sink})

    stage.execute(etl_batch)
  end
end
