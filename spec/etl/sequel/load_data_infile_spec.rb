require 'spec_helper'

describe Chicago::ETL::SequelExtensions::LoadDataInfile do
  before :each do
    @sql = TEST_DB[:foo].load_csv_infile_sql("bar.csv", [:bar, :baz])
  end

  it "loads the data in the file" do
    @sql.should include("LOAD DATA INFILE 'bar.csv'")
  end

  it "replaces rows currently in the table" do
    @sql.should include("REPLACE INTO TABLE `foo`")
  end

  it "should be in the UTF 8 character set" do
    @sql.should include("CHARACTER SET 'utf8'")
  end

  it "should escape with the \" character" do
    @sql.should include("ESCAPED BY '\"'")
  end

  it "supports standard csv, with optional quoting" do
    @sql.should include("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")
  end

  it "loads into the columns specified" do
    @sql.should include("(`bar`,`baz`);")
  end

  it "can ignore instead of replacing rows" do
    @sql = TEST_DB[:foo].insert_ignore.
      load_csv_infile_sql("bar.csv", [:bar, :baz])
    @sql.should include("IGNORE INTO TABLE `foo`")
  end
end
