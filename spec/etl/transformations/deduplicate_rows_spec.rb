require 'spec_helper'

describe Chicago::ETL::DeduplicateRows do
  it "deduplicates rows" do
    class TestTransform < described_class
      def merge_rows(row)
        working_row.merge(row)
      end

      def same_row?(row)
        working_row[:id] == row[:id]
      end
    end

    transform = TestTransform.new

    transform.process({:id => 1, :foo => :bar}).should be_blank
    transform.process({:id => 1, :bar => :baz}).should be_blank
    transform.process({:id => 2, :foo => :quux}).should == [{:id => 1, :foo => :bar, :bar => :baz}]
    transform.process({:id => 3, :foo => :quux}).should == [{:id => 2, :foo => :quux}]

    transform.flush.should == [{:id => 3, :foo => :quux}]
  end
end
