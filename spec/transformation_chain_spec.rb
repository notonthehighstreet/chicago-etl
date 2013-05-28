require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Chicago::Flow::TransformationChain do
  let(:add_1_to_a) {
    Class.new(Chicago::Flow::Transformation) {
      def process_row(row)
        row[:a] += 1
        row
      end
    }
  }

  let(:dup_row) {
    Class.new(Chicago::Flow::Transformation) {
      def output_streams
        [:default, @options[:onto]].flatten
      end

      def process_row(row)
        new_row = assign_stream(row.dup, @options[:onto])
        [row, new_row]
      end
    }
  }

  let(:filter_all) {
    Class.new(Chicago::Flow::Transformation) {
      def process_row(row)
      end
    }
  }

  let(:store_until_flush) {
    Class.new(Chicago::Flow::Transformation) {
      def process_row(row)
        @cache ||= []
        @cache << row
        nil
      end

      def flush
        @cache
      end
    }
  }

  it "chains transformations" do
    described_class.new(add_1_to_a.new, add_1_to_a.new).process({:a => 1}).
      should == [{:a => 3}]
  end

  it "can cope with multiple return rows from transformations" do
    described_class.new(add_1_to_a.new, dup_row.new, add_1_to_a.new).process({:a => 1}).
      should == [{:a => 3}, {:a => 3}]
  end

  it "can cope with a filter returning nil" do
    described_class.new(filter_all.new, dup_row.new, add_1_to_a.new).process({:a => 1}).
      should == []
  end

  it "can write to different streams" do
    described_class.new(dup_row.new(:onto => :other),
                        add_1_to_a.new).process({:a => 1}).
      should == [{:a => 2}, {:a => 1, Chicago::Flow::STREAM => :other}]
  end

  it "knows what streams it writes to as a chain" do
    described_class.new(dup_row.new(:onto => :other),
                        add_1_to_a.new).output_streams.should == [:default, :other]
  end

  it "can flush rows held back by transforms" do
    chain = described_class.new(store_until_flush.new,
                                add_1_to_a.new,
                                store_until_flush.new,
                                add_1_to_a.new)
    chain.process({:a => 1}).should == []
    chain.process({:a => 2}).should == []
    chain.flush.should == [{:a => 3}, {:a => 4}]
  end
end
