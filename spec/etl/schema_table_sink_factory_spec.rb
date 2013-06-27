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

  let(:sink_class) { Chicago::Flow::MysqlFileSink }

  it "builds a MysqlFileSink" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, [:id, :bar, :baz], {})
    
    described_class.new(db, dimension).sink
  end

  it "allows rows to be ignored instead of replaced" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, [:id, :bar, :baz], {:ignore => true})

    described_class.new(db, dimension).sink(:ignore => true)
  end

  it "allows an explicit filepath to be specified" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, [:id, :bar, :baz], {:filepath => "foo"})

    described_class.new(db, dimension).sink(:filepath => "foo")
  end

  it "can exclude columns from a dimension" do
    sink_class.should_receive(:new).
      with(db, :dimension_foo, [:id, :bar], {})

    described_class.new(db, dimension).sink(:exclude => :baz)
  end

  it "builds the key table sink" do
    sink_class.should_receive(:new).
      with(db, :keys_dimension_foo, [:original_id, :dimension_id], {})

    described_class.new(db, dimension).key_sink()
  end

  it "builds other explicit key table sinks" do
    sink_class.should_receive(:new).
      with(db, :keys_foo, [:original_id, :dimension_id], {})

    described_class.new(db, dimension).key_sink(:table => :keys_foo)
  end

  it "builds an error sink" do
    sink_class.should_receive(:new).
      with(db, :etl_error_log, [:column, :row_id, :error, :severity, :error_detail], {}).and_return(stub.as_null_object)

    described_class.new(db, dimension).error_sink
  end
end
