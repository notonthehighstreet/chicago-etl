require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Chicago::Flow::Transformation do
  let(:add_1_to_a) {
    Class.new(Chicago::Flow::Transformation) {
      def process_row(row)
        row[:a] += 1
        row
      end
    }
  }
  
  it "writes to the :default stream by default" do
    subject.output_streams.should == [:default]
  end

  it "may apply to a particular stream" do
    subject.applies_to_stream?(:default).should be_true
    subject.applies_to_stream?(nil).should be_true
    described_class.new(:other).applies_to_stream?(:default).should be_false
    described_class.new(:other).applies_to_stream?(:other).should be_true
  end

  it "processes a row via #process_row" do
    add_1_to_a.new.process({:a => 1}).should == {:a => 2}
  end

  it "passes through rows not on its stream" do
    add_1_to_a.new(:other).process({:a => 1}).should == {:a => 1}
  end

  it "can apply to all streams using :all" do
    add_1_to_a.new(:all).process({:a => 1}).should == {:a => 2}
    add_1_to_a.new(:all).process({:a => 1, Chicago::Flow::STREAM => :other}).
      should == {:a => 2, Chicago::Flow::STREAM => :other}
  end

  it "can be flushed" do
    subject.flush.should == []
  end
end
