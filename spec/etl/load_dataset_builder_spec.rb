require 'spec_helper'

describe Chicago::ETL::LoadDatasetBuilder do
  let(:db) { stub(:database).as_null_object }

  before :each do
    db.stub(:[]).with(:original_users).
      and_return(TEST_DB[:original_users])
    db.stub(:[]).with(:original_preferences).
      and_return(TEST_DB[:original_preferences])
    db[:original_users].stub(:columns).
      and_return([:id, :name, :email])
    db[:original_preferences].stub(:columns).
      and_return([:id, :name, :email])
  end

  it "selects from the specified table" do
    subject.table(:original_users)
    subject.build(db, [:name]).opts[:from].should == [:original_users]
  end

  it "selects the columns from the table" do
    subject.configure do
      table(:original_users)
    end

    subject.build(db, [:id, :name]).opts[:select].should == [:id, :name]
  end

  it "can handle column renaming" do
    subject.configure do
      table :original_users
      rename :id, :original_id
    end

    subject.build(db, [:original_id, :name]).opts[:select].
      should == [:id.as(:original_id), :name]
  end

  it "left outer joins a denormalized table" do
    subject.configure do
      table :original_users
      denormalize :original_preferences, :id => :id
    end

    subject.build(db, [:id, :name]).sql.should =~ /LEFT OUTER JOIN `original_preferences` ON \(`original_preferences`.`id` = `original_users`.`id`\)/
  end

  it "takes columns from the appropriate tables where possible" do
    subject.configure do
      table :original_users
      denormalize :original_preferences, :id => :id
    end

    subject.build(db, [:id, :name, :spam]).opts[:select].
      should include(:spam.qualify(:original_preferences))
  end

  def columns(*syms)
    syms.map {|sym| [sym, {}] }
  end
end
