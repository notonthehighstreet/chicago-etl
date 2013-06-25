require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Chicago::Flow::Transformation do
  let(:add_1_to_a) {
    Class.new(described_class) {
      def process_row(row)
        row[:a] += 1
        row
      end
    }
  }

  let(:add_and_remove) {
    Class.new(described_class) {
      adds_fields :b, :c
      removes_fields :a
      
      def process_row(row)
        row.delete(:a)
        row[:b] = 1
        row[:c] = 2
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

  it "can specify which fields are added" do
    add_and_remove.new.added_fields.should == [:b, :c]
  end

  it "can specify which fields are removed" do
    add_and_remove.new.removed_fields.should == [:a]
  end

  it "can calculate downstream fields" do
    Set.new(add_and_remove.new.downstream_fields([:a, :b, :d])).
      should == Set.new([:b, :c, :d])
  end

  it "can calculate upstream fields" do
    Set.new(add_and_remove.new.upstream_fields([:b, :c, :d])).
      should == Set.new([:a, :d])
  end

  it "has an empty array of added fields by default" do
    subject.added_fields.should == []
  end

  it "has an empty array of removed fields by default" do
    subject.removed_fields.should == []
  end

  it "has an empty array of required options by default" do
    subject.required_options.should == []
  end

  it "can enforce options" do
    klass = Class.new(described_class) { requires_options :foo }
    expect { klass.new }.to raise_error(ArgumentError)
    expect { klass.new(:foo => :bar) }.to_not raise_error(ArgumentError)
  end
end
