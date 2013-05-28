require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe ArraySink do
  it "stores rows in #data" do
    subject << {:a => 1}
    subject.data.should == [{:a => 1}]
  end
end
