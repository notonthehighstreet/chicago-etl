require 'spec_helper'

describe Chicago::ETL::DatasetBatchStage do
  let(:pipeline_stage) { mock(:pipeline_stage).as_null_object }
  let(:dataset) { mock(:dataset).as_null_object }
  let(:stage) { described_class.new(:foo, :source => dataset, :pipeline_stage => pipeline_stage) }
  let(:etl_batch) { stub(:etl_batch) }

  it "has a name" do
    stage.name.should == :foo
  end

  it "should set the inserted at time on the default sink" do
    sink = Chicago::Flow::ArraySink.new(:foo)
    pipeline_stage.stub(:sink).with(:default).and_return(sink)
    stage.pipeline_stage.should == pipeline_stage

    sink.constant_values[:_inserted_at].should_not be_nil
  end

  it "executes the pipeline stage using a DatasetSource" do
    pipeline_stage.should_receive(:execute).
      with(kind_of(Chicago::Flow::DatasetSource))
    stage.execute(etl_batch, true)
  end

  it "truncates any sinks if truncate_pre_load has been set" do
    stage = described_class.new(:foo, :source => dataset, :pipeline_stage => pipeline_stage,
                                :truncate_pre_load => true)

    sink = Chicago::Flow::ArraySink.new(:output)
    sink << {:foo => "foo"}
    pipeline_stage.stub(:sinks).and_return([sink])
    stage.execute(etl_batch)
    sink.data.should == []
  end
end
