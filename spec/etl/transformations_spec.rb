require 'spec_helper'

describe Chicago::ETL::Transformations::DemultiplexErrors do
  it "declares it adds things to the error stream" do
    subject.output_streams.should include(:error)
  end

  it "does nothing to a row without an :_errors key" do
    subject.process_row({}).should == [{}]
  end

  it "removes the :_error key from the row" do
    subject.process_row(:_errors => [{:error => 1}]).first.should == {}
  end

  it "adds the errors onto the error stream" do
    subject.process_row(:_errors => [{:error => 1}]).last.should == {
      :error => 1,
      Chicago::Flow::STREAM => :error
    }
  end
end

describe Chicago::ETL::Transformations::WrittenRowFilter do
  it "only lets the first row through" do
    filter = described_class.new(:key => :id)
    filter.process_row(:id => 1).should == {:id => 1}
    filter.process_row(:id => 2).should == {:id => 2}
    filter.process_row(:id => 1).should be_nil
  end
end

describe Chicago::ETL::Transformations::AddKey do
  let(:key_builder) { stub(:key_builder, :key => 42) }
  let(:transform) { described_class.new(:key_builder => key_builder) }

  it "adds the key to the row" do
    transform.process_row({}).should == {:id => 42}
  end

  it "does not override a key already present" do
    transform.process_row(:id => 1).should == {:id => 1}
  end

  it "adds the key to any rows in an embedded :_errors key" do
    transform.process_row({:_errors => [{}]}).
      should == {:id => 42, :_errors => [{:row_id => 42}]}
  end

  it "should declare that it adds the :id field" do
    transform.added_fields.should == [:id]
  end
end
