require 'spec_helper'

describe Chicago::ETL::SchemaTableSinkFactory do
  let(:db) { stub(:db) }

  let(:dimension) {
    Chicago::Schema::Builders::DimensionBuilder.new(stub(:schema)).build(:foo) do
      columns do
        string :bar
        integer :baz
      end
    end
  }

  let(:sink_class) { Chicago::ETL::MysqlFileSink }

  it "builds a MysqlFileSink" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, {})
    
    described_class.new(db, dimension).sink
  end

  it "allows rows to be ignored instead of replaced" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, :ignore => true)

    described_class.new(db, dimension).sink(:ignore => true)
  end

  it "allows an explicit filepath to be specified" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, :filepath => "foo")

    described_class.new(db, dimension).sink(:filepath => "foo")
  end

  it "can exclude columns from a dimension" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, {})

    described_class.new(db, dimension).sink(:exclude => :baz)
  end

  it "builds the key table sink" do
    sink = stub(:sink).as_null_object
    sink_class.should_receive(:new).
      with(db, :keys_dimension_foo, {}).
      and_return(sink)
    sink.should_receive(:set_columns).with(:original_id, :dimension_id).
      and_return(sink)

    described_class.new(db, dimension).key_sink()
  end

  it "builds other explicit key table sinks" do
    sink = stub(:sink).as_null_object
    sink_class.should_receive(:new).
      with(db, :keys_foo, {}).
      and_return(sink)
    sink.should_receive(:set_columns).with(:original_id, :dimension_id).
      and_return(sink)
    
    described_class.new(db, dimension).key_sink(:table => :keys_foo)
  end

  it "builds an error sink" do
    sink = stub(:sink).as_null_object
    sink_class.should_receive(:new).
      with(db, :etl_error_log, {}).and_return(sink)
    sink.should_receive(:set_columns).with(:column, :row_id, :error, :severity, :error_detail).
      and_return(sink)

    described_class.new(db, dimension).error_sink
  end
end
