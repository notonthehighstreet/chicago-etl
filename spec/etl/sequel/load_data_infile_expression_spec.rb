require 'spec_helper'

describe Chicago::ETL::SequelExtensions::LoadDataInfileExpression do
  it "loads the data in the file into the table" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("LOAD DATA INFILE 'bar.csv' INTO TABLE `foo`")
  end

  it "loads the data with replacment" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'],
                        :update => :replace).
      to_sql(TEST_DB).should include("REPLACE INTO TABLE")
  end

  it "loads the data ignoring rows" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :update => :ignore).
      to_sql(TEST_DB).should include("IGNORE INTO TABLE")
  end

  it "should be in UTF-8 character set by default" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("CHARACTER SET 'utf8'")
  end

  it "may be in other character sets" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :character_set => "ascii").
      to_sql(TEST_DB).should include("CHARACTER SET 'ascii'")
  end

  it "should load columns" do
    described_class.new("bar.csv", :foo, ['bar', 'quux']).
      to_sql(TEST_DB).should include("(`bar`,`quux`)")
  end

  it "should load into variables if column begins with @" do
    described_class.new("bar.csv", :foo, ['@bar', 'quux']).
      to_sql(TEST_DB).should include("(@bar,`quux`)")
  end

  it "can ignore lines" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :ignore => 2).
      to_sql(TEST_DB).should include("IGNORE 2 LINES")
  end

  it "can be in csv format" do
    described_class.new("bar.csv", :foo, ['bar', 'quux'], :format => :csv).
      to_sql(TEST_DB).should include("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"'")
  end

  it "can set column values" do
    sql = described_class.new("bar.csv", :foo, ['@bar', 'quux'], 
                        :set => {:bar => :unhex.sql_function("@bar".lit),
                        :etl_batch_id => 3}).
      to_sql(TEST_DB)

    sql.should include("SET")
    sql.should include("`etl_batch_id` = 3")
    sql.should include("`bar` = unhex(@bar)")
  end
end
