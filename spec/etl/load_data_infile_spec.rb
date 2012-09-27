require 'spec_helper'

describe Chicago::SequelExtensions::LoadDataInfile do
  it "should load load the data in file to the table" do
    TEST_DB[:foo].load_csv_infile_sql("bar.csv", [:bar, :baz]).
      should == "LOAD DATA INFILE 'bar.csv' REPLACE INTO TABLE `foo` CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' (`bar`,`baz`);"
  end
end
