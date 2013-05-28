require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe ArraySource do
  it "has an each method that yields rows" do
    ArraySource.new([{:a => 1}]).each do |row|
      row.should == {:a => 1}
    end
  end

  it "doesn't know about any fields rows have by default" do
    ArraySource.new([]).fields.should == []
    ArraySource.new([]).should_not have_defined_fields
  end
  
  it "can optionally define which fields will be in rows" do
    ArraySource.new([], [:a, :b]).fields.should == [:a, :b]
    ArraySource.new([], :a).fields.should == [:a]
    ArraySource.new([], :a).should have_defined_fields
  end
end
