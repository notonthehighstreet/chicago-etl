require 'spec_helper'

describe Chicago::ETL::SequelExtensions::FilterToEtlBatch do
  let(:batch) { stub(:batch, :id => 42) }

  it "should do nothing to a table without an etl_batch_id column" do
    TEST_DB.should_receive(:schema).with(:foo).and_return([])
    TEST_DB[:foo].filter_to_etl_batch(batch).should == TEST_DB[:foo]
  end

  it "filters a table with an ETL batch id column" do
    TEST_DB.should_receive(:schema).with(:foo).and_return([[:etl_batch_id, {}]])
    TEST_DB[:foo].filter_to_etl_batch(batch).sql.
      should include("\(`foo`.`etl_batch_id` = 42\)")
  end

  it "filters an aliased table with an ETL batch id column" do
    TEST_DB.should_receive(:schema).with(:foo).and_return([[:etl_batch_id, {}]])
    TEST_DB[:foo.as(:bar)].filter_to_etl_batch(batch).sql.
      should include("\(`bar`.`etl_batch_id` = 42\)")
  end

  it "doesn't attempt to look for etl columns in nested queries" do
    TEST_DB[TEST_DB[:foo].as(:bar)].filter_to_etl_batch(batch).sql.
      should_not include("`bar`.`etl_batch_id` = 42")
  end

  it "filters based on joins" do
    TEST_DB.should_receive(:schema).with(:baz).and_return([[:etl_batch_id, {}]])
    TEST_DB.should_receive(:schema).with(:bar).and_return([])
    TEST_DB.should_receive(:schema).with(:foo).and_return([])

    sql = TEST_DB[:foo].join_table(:left_outer, :bar, :id => :id).join(:baz).filter_to_etl_batch(batch).sql
    sql.should include("\(`baz`.`etl_batch_id` = 42\)")
  end

  it "filters based on joined aliases" do
    TEST_DB.should_receive(:schema).with(:bar).and_return([[:etl_batch_id, {}]])
    TEST_DB.should_receive(:schema).with(:foo).and_return([])

    TEST_DB[:foo].join(:bar.as(:baz)).filter_to_etl_batch(batch).sql.
      should include("\(`baz`.`etl_batch_id` = 42\)")
  end

  it "applies filters to each unioned dataset" do
    TEST_DB.should_receive(:schema).with(:bar).and_return([[:etl_batch_id, {}]])
    TEST_DB.should_receive(:schema).with(:foo).and_return([[:etl_batch_id, {}]])

    sql = TEST_DB[:foo].union(TEST_DB[:bar], :from_self => false).filter_to_etl_batch(batch).sql

    sql.should include("\(`foo`.`etl_batch_id` = 42\)")
    sql.should include("\(`bar`.`etl_batch_id` = 42\)")
  end
end
