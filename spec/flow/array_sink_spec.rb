require 'spec_helper'

describe ArraySink do
  let(:sink) { described_class.new(:foo) }

  it "has a name" do
    sink.name.should == :foo
  end

  it "stores rows in #data" do
    sink << {:a => 1}
    sink.data.should == [{:a => 1}]
  end

  it "merges constant values into the sink row" do
    sink.set_constant_values(:number => 1).should == sink
    sink << {:a => 1}
    sink.data.should == [{:a => 1, :number => 1}]
  end

  it "can be truncated" do
    sink << {:a => 1}
    sink.truncate
    sink.data.should be_empty
  end
end
