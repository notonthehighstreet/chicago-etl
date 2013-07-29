require 'spec_helper'

describe Chicago::Flow::ArraySource do
  it "has an each method that yields rows" do
    described_class.new([{:a => 1}]).each do |row|
      row.should == {:a => 1}
    end
  end

  it "doesn't know about any fields rows have by default" do
    described_class.new([]).fields.should == []
    described_class.new([]).should_not have_defined_fields
  end
  
  it "can optionally define which fields will be in rows" do
    described_class.new([], [:a, :b]).fields.should == [:a, :b]
    described_class.new([], :a).fields.should == [:a]
    described_class.new([], :a).should have_defined_fields
  end
end
