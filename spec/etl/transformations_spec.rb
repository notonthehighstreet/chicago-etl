require 'spec_helper'

describe Chicago::ETL::Transformations::DemultiplexErrors do
  it "declares it adds things to the error stream" do
    subject.output_streams.should include(:error)
  end

  it "does nothing to a row without an :_errors key" do
    subject.process({}).should == [{}]
  end

  it "removes the :_error key from the row" do
    subject.process(:_errors => [{:error => 1}]).first.should == {}
  end

  it "adds the errors onto the error stream" do
    subject.process(:_errors => [{:error => 1}]).last.should == {
      :error => 1,
      Chicago::ETL::STREAM => :error
    }
  end
end

describe Chicago::ETL::Transformations::WrittenRowFilter do
  it "only lets the first row through" do
    filter = described_class.new(:key => :id)
    filter.process(:id => 1).should == {:id => 1}
    filter.process(:id => 2).should == {:id => 2}
    filter.process(:id => 1).should be_nil
  end

  it "requires a key option" do
    described_class.required_options.should include(:key)
  end
end

describe Chicago::ETL::Transformations::AddKey do
  let(:key_builder) { double(:key_builder, :key => 42) }
  let(:transform) { described_class.new(:key_builder => key_builder) }

  it "requires a key builder" do
    described_class.required_options.should include(:key_builder)
  end

  it "adds the key to the row" do
    transform.process({}).should == {:id => 42}
  end

  it "adds the key to any rows in an embedded :_errors key" do
    transform.process({:_errors => [{}]}).
      should == {:id => 42, :_errors => [{:row_id => 42}]}
  end

  it "should declare that it adds the :id field" do
    transform.added_fields.should == [:id]
  end

  it "should declare that it writes to the dimension_key stream" do
    transform.output_streams.should include(:dimension_key)
  end

  it "should return a new row on the dimension_key stream" do
    key_builder.stub(:key => [42, {:original_id => 42}])
    transform.process({}).last.should == {:_stream => :dimension_key, :original_id => 42}
  end
end

describe Chicago::ETL::Transformations::DimensionKeyMapping do
  let(:transform) {
    described_class.new(:original_key => :original_id,
                        :key_table => :keys_foo)
  }
  
  it "should require an original_key and a key table" do
    described_class.required_options.should == [:original_key, :key_table]
  end

  it "removes the key from the stream" do
    transform.process({:original_id => 1}).first.should == {}
    transform.removed_fields.should == [:original_id]
  end

  it "links the original key with the id on the stream" do
    transform.process({:original_id => 1, :id => 2}).last.
      should == {:_stream => :keys_foo, :original_id => 1, :dimension_id => 2}
  end
end

describe Chicago::ETL::Transformations::HashColumns do
  it "requires a columns option" do
    described_class.required_options.should include(:columns)
  end

  it "adds a hash field to the row" do
    Digest::MD5.stub(:hexdigest).with("ab").and_return("a")

    transform = described_class.new(:columns => [:a, :b])
    transform.added_fields.should == [:hash]
    transform.process(:a => 'a', :b => 'b')[:hash].should == "A"
  end

  it "can add the hash to an arbitrary output field" do
    Digest::MD5.stub(:hexdigest).with("ab").and_return("a")
    transform = described_class.new(:columns => [:a, :b], 
                                    :output_field => :foo)
    transform.added_fields.should == [:foo]
    transform.process(:a => 'a', :b => 'b')[:foo].should == "A"
  end
end
