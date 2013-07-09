require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe PipelineStage do
  let(:transform) {
    Class.new(Transformation) {
      def process_row(row)
        row[:a] += 1
        row
      end
    }
  }
  
  let(:add_error) {
    Class.new(Transformation) {
      # add_output_stream :error
      def output_streams
        [:default, :error]
      end
      
      def process_row(row)
        [row, {STREAM => :error, :message => "error"}]
      end
    }
  }

  let(:sink) { ArraySink.new(:test) }
  let(:source) { ArraySource.new([{:a => 1}]) }

  it "returns all sinks" do
    stage = described_class.new.register_sink(:default, sink)
    stage.sinks.should == [sink]
  end

  it "returns a sink by name" do
    stage = described_class.new.register_sink(:default, sink)
    stage.sink(:default).should == sink
  end

  it "reads from source to sink" do
    pipeline = described_class.new.register_sink(:default, sink)
    pipeline.execute(source)
    sink.data.should == [{:a => 1}]
  end

  it "passes rows through transforms" do
    pipeline = described_class.new(:transformations => [transform.new]).
      register_sink(:default, sink)
                                   
    pipeline.execute(source)
    sink.data.should == [{:a => 2}]
  end

  it "writes rows to the appropriate sink for their stream, and strips the stream tag" do
    error_sink = ArraySink.new(:test)

    pipeline = described_class.new(:transformations => [add_error.new]).
      register_sink(:default, sink).
      register_sink(:error, error_sink)

    pipeline.execute(source)
    sink.data.should == [{:a => 1}]
    error_sink.data.should == [{:message => "error"}]
  end

  it "calls an error handler if sinks are not registered" do
    error_handler = mock(:error_handler)
    error_handler.should_receive(:unregistered_sinks).
      with([:default, :error])

    pipeline = described_class.new(:transformations => [add_error.new],
                                   :error_handler => error_handler)

    pipeline.validate_pipeline
  end

  it "by default raises an exception if the pipeline is not valid when executed" do
    pipeline = described_class.new(:transformations => [add_error.new])
    expect { pipeline.execute(source) }.to raise_error(Chicago::Flow::Error)
  end

  it "opens sinks before writing and closes them afterwards" do
    sink = mock(:sink)
    pipeline = described_class.new.register_sink(:default, sink)
    sink.should_receive(:open)
    sink.stub(:<<)
    sink.should_receive(:close)
    pipeline.execute(source)
  end
end
