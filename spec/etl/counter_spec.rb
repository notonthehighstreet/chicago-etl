require 'spec_helper'

describe Chicago::ETL::Counter do
  it "returns the next available key" do
    key = described_class.new(3)
    key.next.should == 4
    key.next.should == 5
  end

  it "can have the initial key set via a block" do
    counter = described_class.new { 1 + 1 }
    counter.next.should == 3
  end

  it "defaults the counter to 0 if the block returns nil" do
    counter = described_class.new { nil }
    counter.next.should == 1
  end

  it "prefers the block to the argument for setting initial state" do
    counter = described_class.new(5) { 2 }
    counter.next.should == 3
  end

  it "can be constructed with no argument, implying 0" do
    described_class.new.next.should == 1
  end

  it "updates keys in a thread-safe fashion" do
    key = described_class.new

    # These seem to need to be a fairly large number of times to see
    # errors
    [Thread.new { 100_000.times {|i| key.next } },
     Thread.new { 100_000.times {|i| key.next } },
     Thread.new { 100_000.times {|i| key.next } }].each(&:join)

    key.next.should == 300_001
  end

  it "has a current value" do
    described_class.new.current.should == 0
  end
end
