require "spec_helper"

TEST_SINK = Chicago::Flow::ArraySink.new(:test)
TEST_ERROR_SINK = Chicago::Flow::ArraySink.new(:error_test)

class TestTransformation < Chicago::Flow::Transformation
  def output_streams
    [:another_stream]
  end

  def process_row(row)
    [row, assign_stream({:some_field => "has an error value"}, :another_stream)]
  end
end

describe "defining and running a stage" do
  let(:rows) { [{:some_field => "value"}] } 
  let(:db) { double(:test_dataset_method => rows) }
  let(:schema) { double }
  let(:pipeline) { Chicago::ETL::Pipeline.new(db, schema)}

  it "allows a user of the library to define a stage" do
    pipeline.define_stage(:test_stage) do
    end
  end

  it "allows no tranformations or sinks or source??" do

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
        add TEST_SINK
        add TEST_ERROR_SINK, :stream => :another_stream
      end
    end

    pipeline.stages.each do |stage|
      stage.execute(double, true)
    end

    TEST_SINK.data.length.should == 1
    TEST_SINK.data.first.should == {:some_field => "value"}

    TEST_ERROR_SINK.data.length.should == 1
    TEST_ERROR_SINK.data.first.should == {:some_field => "has an error value"}
  end
end
