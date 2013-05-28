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

  let(:sink) { [] }
  let(:source) { [{:a => 1}] }

  it "reads from source to sink" do
    pipeline = described_class.new(source, :sinks => {:default => sink})
    pipeline.execute
    sink.should == source
  end

  it "passes rows through transforms" do
    pipeline = described_class.new(source,
                                   :sinks => {:default => sink},
                                   :transformations => [transform.new])
    pipeline.execute
    sink.should == [{:a => 2}]
  end

  it "writes rows to the appropriate sink for their stream, and strips the stream tag" do
    error_sink = []

    pipeline = described_class.new(source,
                                   :sinks => {:default => sink, :error => error_sink},
                                   :transformations => [add_error.new])

    pipeline.execute
    sink.should == [{:a => 1}]
    error_sink.should == [{:message => "error"}]
  end

  it "calls an error handler if sinks are not registered" do
    error_handler = mock(:error_handler)
    error_handler.should_receive(:unregistered_sinks).with([:default, :error])

    pipeline = described_class.new(source,
                                   :transformations => [add_error.new],
                                   :error_handler => error_handler)

    pipeline.validate_pipeline
  end

  it "by default raises an exception if the pipeline is not valid when executed" do
    pipeline = described_class.new(source,
                                   :transformations => [add_error.new])

    expect { pipeline.execute }.to raise_error(Chicago::Flow::Error)
  end
end
