require 'spec_helper'

describe Hash do
  it "should have a put method which returns the hash" do
    {}.put(:a, 1).should == {:a => 1}
  end

  it "should have a modify existing method that ignores nil values" do
    {:a => nil}.modify_existing(:a) {|v| 2 }.should == {:a => nil}
    {:a => 1}.modify_existing(:a) {|v| 2 }.should == {:a => 2}
    {}.modify_existing(:a) {|r| 2 }.should == {}
  end
end
