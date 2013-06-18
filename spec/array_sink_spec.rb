require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe ArraySink do
  it "stores rows in #data" do
    subject << {:a => 1}
    subject.data.should == [{:a => 1}]
  end

  it "merges constant values into the sink row" do
    subject.set_constant_values(:number => 1).should == subject
    subject << {:a => 1}
    subject.data.should == [{:a => 1, :number => 1}]
  end
end
