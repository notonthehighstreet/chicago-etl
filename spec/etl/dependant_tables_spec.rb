require 'spec_helper'

describe Chicago::SequelExtensions::DependantTables do
  it "returns the table in the from clause" do
    TEST_DB[:foo].dependant_tables.should == [:foo]
  end

  it "returns tables from join clauses" do
    TEST_DB[:foo].join(:bar).join(:baz).dependant_tables.
      should == [:foo, :bar, :baz]
  end

  it "returns unique real tables from join clauses when aliased" do
    TEST_DB[:foo].join(:bar).join(:bar.as(:baz)).dependant_tables.
      should == [:foo, :bar]
  end

  it "returns real tables from 'from' clauses when aliased" do
    TEST_DB[:foo.as(:bar)].join(:bar).join(:bar.as(:baz)).
      dependant_tables.should == [:foo, :bar]
  end

  it "returns tables from nested datasets in the from clause" do
    TEST_DB[TEST_DB[:foo].as(:bar)].dependant_tables.should == [:foo]
  end

  it "returns tables from nested datasets in the join clause" do
    TEST_DB[:foo].join(TEST_DB[:bar].as(:baz)).dependant_tables.
      should == [:foo, :bar]
  end

  it "handles unioned datasets" do
    TEST_DB[:foo].union(TEST_DB[:bar]).union(TEST_DB[:baz]).
      dependant_tables.should == [:foo, :bar, :baz]
  end

  it "handles unioned datasets where from_self is false" do
    TEST_DB[:foo].union(TEST_DB[:bar], :from_self => false).
      dependant_tables.should == [:foo, :bar]
  end
end
