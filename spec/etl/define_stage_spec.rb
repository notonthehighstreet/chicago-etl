require "spec_helper"

class TestTransformation < Chicago::Flow::Transformation
  def output_streams
    [:another_stream]
  end

  def process_row(row)
    [row, assign_stream({:some_field => "has an error value"}, :another_stream)]
  end
end

describe "defining and executing a stage" do
  let(:rows) { [{:some_field => "value"}] } 
  let(:db) { double(:test_dataset_method => rows) }
  let(:schema) { double }
  let(:pipeline) { Chicago::ETL::Pipeline.new(db, schema)}

  it "allows no tranformations" do
    pipeline.define_stage(:test_stage) do
      source do
        db.test_dataset_method
      end

      sinks do
        add Chicago::Flow::ArraySink.new(:test)
        add Chicago::Flow::ArraySink.new(:test), :stream => :another_stream
      end
    end

    pipeline.stages.each do |stage|
      stage.execute(double, true)
    end

    stage = pipeline.stages.first
    stage.sink(:default).data.length.should == 1
    stage.sink(:default).data.first.should == {:some_field => "value"}

    stage.sink(:another_stream).data.length.should == 0
  end

  it "requires sinks" do
    expect {
      pipeline.define_stage(:test_stage) do
        source do
          db.test_dataset_method
        end
      end
    }.to raise_error(ArgumentError)
  end
  
  it "requires sources" do
    expect {
      pipeline.define_stage(:test_stage) do
        sinks do
          add Chicago::Flow::ArraySink.new(:test)
        end
      end
    }.to raise_error(ArgumentError)
  end

  it "glues the source, transformations, and sink correctly" do
    pipeline.define_stage(:test_stage) do
      source do
        db.test_dataset_method
      end

      transformations do
        add TestTransformation.new
      end

      sinks do
        add Chicago::Flow::ArraySink.new(:test)
        add Chicago::Flow::ArraySink.new(:test), :stream => :another_stream
      end
    end

    pipeline.stages.each do |stage|
      stage.execute(double, true)
    end

    stage = pipeline.stages.first
    stage.sink(:default).data.length.should == 1
    stage.sink(:default).data.first.should == {:some_field => "value"}

    stage.sink(:another_stream).data.length.should == 1
    stage.sink(:another_stream).data.first.should == {:some_field => "has an error value"}
  end

  it "allows the source to be filtered via a filter strategy" do
    etl_batch_double = double
    fake_source = []
    
    fake_source.should_receive(:another_dataset_method).and_return([])    
    pipeline.define_stage(:test_stage) do
      source do
        fake_source
      end

      sinks do
        add Chicago::Flow::ArraySink.new(:test)
      end

      filter_strategy do |source, etl_batch|
        etl_batch.should == etl_batch_double
        source.another_dataset_method
      end
    end
    
    pipeline.stages.each do |stage|
      stage.execute(etl_batch_double, false)
    end
  end
end
