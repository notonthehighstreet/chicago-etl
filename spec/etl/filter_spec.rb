require 'spec_helper'

describe Chicago::ETL::Filter do
  it "filters all rows by default" do
    subject.process({:a => 1}).should == []
  end

  it "filters rows given a block" do
    filter = described_class.new {|row| row.has_key?(:a) }
    filter.process(:a => 1).should == [{:a => 1}]
    filter.process(:b => 1).should == []
  end
end
