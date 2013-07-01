require 'spec_helper'

describe Chicago::ETL::DatasetBatchStage do
  let(:pipeline_stage) { mock(:pipeline_stage).as_null_object }
  let(:dataset) { mock(:dataset).as_null_object }
  let(:stage) { described_class.new(dataset, pipeline_stage) }
  let(:etl_batch) { stub(:etl_batch) }

  it "should set the inserted at time on the default sink" do
    sink = Chicago::Flow::ArraySink.new(:foo)
    pipeline_stage.stub(:sink).with(:default).and_return(sink)
    stage.execute(stub(:etl_batch))

    sink.constant_values[:_inserted_at].should_not be_nil
  end

  it "filters the dataset to the batch" do
    dataset.should_recieve(:filter_to_etl_batch).with(etl_batch)
    stage.execute(etl_batch)
  end

  it "does not filter the dataset if re-extracting" do
    dataset.should_not_recieve(:filter_to_etl_batch)
    stage.execute(etl_batch, true)
  end

  it "executes the pipeline stage using a DatasetSource" do
    pipeline_stage.should_receive(:execute).
      with(kind_of(Chicago::Flow::DatasetSource))
    stage.execute(etl_batch, true)
  end
end
