require 'spec_helper'

describe Chicago::ETL::ArraySource do
  it "has an each method that yields rows" do
    described_class.new([{:a => 1}]).each do |row|
      row.should == {:a => 1}
    end
  end

  it "doesn't know about any columns rows have by default" do
    described_class.new([]).columns.should == []
    described_class.new([]).should_not have_defined_columns
  end
  
  it "can optionally define which columns will be in rows" do
    described_class.new([], [:a, :b]).columns.should == [:a, :b]
    described_class.new([], :a).columns.should == [:a]
    described_class.new([], :a).should have_defined_columns
  end
end
