require 'spec_helper'

describe "Chicago::ETL Execution method" do
  class StubBatch
    def perform_task(*args)
      yield
    end
  end

  let(:logger) { double(:logger).as_null_object }
  let(:batch) { StubBatch.new }

  it "only logs skipping the stage if the stage is not executable" do
    stage = double(:stage, :executable? => false, :name => "test")
    stage.should_not_receive(:execute)
    logger.should_receive(:info).with("Skipping stage test")

    Chicago::ETL.execute(stage, batch, logger)
  end

  it "executes the stage" do
    stage = double(:stage, :executable? => true, :name => "test")
    stage.should_receive(:execute).with(batch)

    Chicago::ETL.execute(stage, batch, logger)
  end
end
