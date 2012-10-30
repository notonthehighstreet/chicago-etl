require 'spec_helper'

describe Chicago::ETL::Transformations::AddInsertTimestamp do
  it "adds a timestamp in UTC in the _inserted_at field" do
    time = subject.call({}).first[:_inserted_at]
    time.should be_kind_of(Time)
    time.zone.should == "UTC"
  end
end
