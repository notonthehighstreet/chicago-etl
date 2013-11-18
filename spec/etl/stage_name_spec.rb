require 'spec_helper'
require 'set'

describe Chicago::ETL::StageName do
  it "can be consturcted with variable args" do
    described_class.new(:a, :b).to_a.should == [:a, :b]
  end

  it "can be constructed with an array of symbols" do
    described_class.new([:a, :b]).to_a.should == [:a, :b]
  end

  it "can be constructed with a dot seaprated string" do
    described_class.new("foo.bar").to_a.should == [:foo, :bar]
  end

  it "has a name" do
    described_class.new("foo.bar.baz").name.should == :baz
  end

  it "has a namespace" do
    described_class.new("foo.bar.baz").namespace.should == [:foo, :bar]
  end

  it "supports equality" do
    described_class.new(:a, :b).should == described_class.new(:a, :b)
    set = Set.new
    set << described_class.new(:a, :b)
    set.should include(described_class.new(:a, :b))
  end

  it "has a dotted string representation" do
    described_class.new(:a, :b).to_s.should == "a.b"
  end

  it "matches an exact pattern" do
    described_class.new(:a, :b).match?(:a, :b).should be_true
    described_class.new(:a, :b).match?(:a, :c).should be_false
  end

  it "matches a left-anchored partial pattern" do
    described_class.new(:a, :b).match?(:a).should be_true
    described_class.new(:a, :b).match?(:b).should be_false
  end

  it "allows wildcards matching" do
    described_class.new(:a, :b).match?(:*, :b).should be_true
    described_class.new(:a, :b).match?(:*, :*).should be_true
    described_class.new(:a, :b).match?(:*, :*, :*).should be_false
  end

  it "can use the =~ operator" do
    (described_class.new(:a, :b) =~ [:*, :b]).should be_true
  end
end
