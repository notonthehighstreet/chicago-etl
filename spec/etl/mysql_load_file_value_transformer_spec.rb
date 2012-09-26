require 'spec_helper'

describe Chicago::ETL::MysqlLoadFileValueTransformer do
  it "transforms nil into \\N" do
    subject.transform(nil).should == "\\N"
  end

  it "transforms true into '1'" do
    subject.transform(true).should == "1"
  end

  it "transforms false into '0'" do
    subject.transform(false).should == "0"
  end

  it "transforms times into mysql time format" do
    subject.transform(Time.local(2011,01,02,10,30,50)).should == "2011-01-02 10:30:50"
  end

  it "transforms datetimes into mysql time format" do
    subject.transform(DateTime.new(2011,01,02,10,30,50)).should == "2011-01-02 10:30:50"
  end

  it "transforms dates into mysql date format" do
    subject.transform(Date.new(2011,01,02)).should == "2011-01-02"
  end
end
