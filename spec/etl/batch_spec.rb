require 'spec_helper'
require 'date'

describe Chicago::ETL::Batch do
  before :each do
    TEST_DB.drop_table(*(TEST_DB.tables))
    ETL::TableBuilder.build(TEST_DB)
    ETL::Batch.db = TEST_DB
    Chicago.project_root = File.expand_path(File.join(File.dirname(__FILE__), ".."))
    tmpdir = File.expand_path(File.join(File.dirname(__FILE__), "..", "tmp"))
    FileUtils.rm_r(tmpdir) if File.exists?(tmpdir)
  end

  it "should return a new batch when instance is called and there are no outstanding batches in error" do
    ETL::Batch.instance.should be_new
  end

  it "should set the start timestamp of the batch to now when created" do
    (ETL::Batch.instance.start.started_at.to_i - Time.now.to_i).abs.
      should <= 5
  end

  it "should have a state of 'Started' when started" do
    ETL::Batch.instance.start.state.should == "Started"
  end

  it "should have a default extracted_to datetime of midnight (this morning)" do
    now = Time.now
    ETL::Batch.instance.start.extracted_to.should == Time.local(now.year, now.month, now.day, 0,0,0)
  end

  it "should be able to specify an extract to date" do
    now = Date.today - 1
    ETL::Batch.instance.start(now).extracted_to.should == Time.local(now.year, now.month, now.day, 0,0,0)
  end

  it "should create a directory tmp/batches/1 under the project root when created" do
    ETL::Batch.instance.start
    File.should be_directory(Chicago.project_root + "/tmp/batches/1")
  end

  it "should return the batch directory path from #dir" do
    ETL::Batch.instance.start.dir.should == Chicago.project_root + "/tmp/batches/1"
  end

  it "should set the finished_at timestamp when #finish is called" do
    batch = ETL::Batch.instance.start
    batch.finish
    batch.finished_at.should_not be_nil
    batch.state.should == "Finished"
  end

  it "should return true from #error? if in the error state" do
    batch = ETL::Batch.instance.start
    batch.error
    batch.should be_in_error
  end

  it "returns nil from extract_from when re-extracting" do
    batch = ETL::Batch.instance
    batch.reextract
    batch.start
    expect(batch.extract_from).to be_nil
  end

  it "returns nil from extract_from when the first batch" do
    batch = ETL::Batch.instance
    batch.start
    expect(batch.extract_from).to be_nil
  end

  it "returns the previous finised batch's extracted_to as extract_from" do
    Timecop.freeze(2014, 01, 6, 0, 0, 0) {
      ETL::Batch.new.start.finish
    }

    Timecop.freeze(2014, 01, 10, 0, 0, 0) {
      ETL::Batch.new.start.finish
    }

    ETL::Batch.new.start.error
    
    batch = ETL::Batch.new.start
    expect(batch.extract_from).to eql(Time.local(2014,1,10,0,0,0))
  end

  it "returns the previous finised batch's extracted_to as extract_from" do
    Timecop.freeze(2014, 01, 6, 0, 0, 0) {
      ETL::Batch.new.start.finish
    }

    Timecop.freeze(2014, 01, 8, 0, 0, 0) {
      ETL::Batch.new.start.error
    }

    Timecop.freeze(2014, 01, 10, 0, 0, 0) {
      ETL::Batch.new.start.finish
    }
    
    batch = ETL::Batch.new.start
    expect(batch.extract_from).to eql(Time.local(2014,1,10,0,0,0))
  end

  it "returns yesterday, rather than extract_from if extract_from is today" do
    Timecop.freeze(2014, 01, 6, 0, 0, 0)

    ETL::Batch.new.start.finish
    
    batch = ETL::Batch.new.start
    expect(batch.extract_from).to eql(Time.local(2014,1,5,0,0,0))
  end

  context "when rerun in the same day" do
    it "should not return a new batch if the last batch was not finished" do
      batch = ETL::Batch.instance.start
      expect(ETL::Batch.instance).to eql(batch)
    end

    it "should not return a new batch if the last batch ended in error" do
      batch = ETL::Batch.instance.start
      batch.error
      ETL::Batch.instance.should == batch
    end
  end

  context "when rerun a day later" do
    it "returns a new batch when the previous batch was unfinished" do
      batch = ETL::Batch.instance.start
      Timecop.freeze(Date.today + 1)
      expect(ETL::Batch.instance).to_not eql(batch)
    end

    it "returns a new batch when the previous batch was in error" do
      batch = ETL::Batch.instance.start
      batch.error
      Timecop.freeze(Date.today + 1)
      expect(ETL::Batch.instance).to_not eql(batch)
    end
  end

  it "should create a log in tmp/batches/1/log" do
    ETL::Batch.instance.start
    File.read(Chicago.project_root + "/tmp/batches/1/log").
      should include("Started ETL batch 1.")
  end

  it "should perform a task only once" do
    batch = ETL::Batch.instance.start
    i = 0
    2.times { batch.perform_task("Transform", "Test") { i += 1} }
    i.should == 1
    batch.task_invocations_dataset.filter(:stage => "Transform", :name => "Test").count.should == 1
  end

  it "should not complain when given a symbol as the stage name" do
    batch = ETL::Batch.instance.start
    lambda { batch.perform_task(:transform, "Test") {} }.should_not raise_error
  end

  it "can be marked as re-extracting" do
    ETL::Batch.instance.reextract.should be_reextracting
    ETL::Batch.instance.should_not be_reextracting
  end
end
