require 'spec_helper'

describe Chicago::ETL::Stage do
  it "requires a source" do
    expect {
      described_class.new(:test,
                          :source => nil,
                          :sinks => {:default => stub(:sink)})
    }.to raise_error(ArgumentError)
  end

  it "requires sinks" do
    expect {
      described_class.new(:test,
                          :source => stub(:source),
                          :sinks => nil)
    }.to raise_error(ArgumentError)
  end

  it "does not filter the dataset if re-extracting" do
    stage = described_class.new(:test,
                                :source => stub(:source),
                                :sinks => {:default => stub(:sink)},
                                :filter_strategy => lambda { fail })

    stage.filtered_source(stub(:etl_batch), true)
  end

  it "opens sinks before writing and closes them afterwards" do
    sink = mock(:sink)
    sink.should_receive(:open)
    sink.should_receive(:close)

    stage = described_class.new(:test,
                                :source => [],
                                :sinks => {:default => sink})

    stage.execute(stub(:etl_batch), true)
  end
end
