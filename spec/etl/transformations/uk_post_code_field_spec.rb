# -*- coding: utf-8 -*-
require 'spec_helper'

describe Chicago::ETL::Transformations::UkPostCodeField do
  it "reformats a 6 character postcode" do
    data = described_class.new.normalize(" SW 30 TA")
    data[:post_code].should == "SW3 0TA"
  end

  it "reformats a 7 character postcode" do
    data = described_class.new.normalize(" G L515 FD")
    data[:post_code].should == "GL51 5FD"
  end

  it "reformats a BFPO postcode" do
    data = described_class.new.normalize("bfpo123")
    data[:post_code].should == "BFPO 123"
  end

  it "returns the outcode" do
    data = described_class.new.normalize(" G L515 FD")
    data[:outcode].should == "GL51"
  end

  it "returns the incode" do
    data = described_class.new.normalize(" G L515 FD")
    data[:outcode].should == "GL51"
  end

  it "fixes people using 0 instead of O for OX and OL post codes" do
    data = described_class.new.normalize(" 0X1 4HG")
    data[:post_code].should == "OX1 4HG"

    data = described_class.new.normalize(" 0L1 4HG")
    data[:post_code].should == "OL1 4HG"
  end

  it "fixes people using 0 instead of O for OX post codes" do
    data = described_class.new.normalize(" 0X1 4HG")
    data[:post_code].should == "OX1 4HG"
  end

  it "fixes people using 0 instead of O for OL post codes" do
    data = described_class.new.normalize(" 0L1 0LG")
    data[:post_code].should == "OL1 0LG"
  end

  it "fixes people using 0 instead of O for PO post codes" do
    data = described_class.new.normalize(" P01 4HG")
    data[:post_code].should == "PO1 4HG"
  end

  it "fixes people using 0 instead of O for SO post codes" do
    data = described_class.new.normalize(" S01 4HG")
    data[:post_code].should == "SO1 4HG"
  end

  it "fixes people using 0 instead of O for CO post codes" do
    data = described_class.new.normalize(" C01 4HG")
    data[:post_code].should == "CO1 4HG"
  end

  it "fixes people using 0 instead of O for YO post codes" do
    data = described_class.new.normalize(" Y01 4HG")
    data[:post_code].should == "YO1 4HG"
  end

  it "fixes shift key slips" do
    data = described_class.new.normalize(")X!£ $HG")
    data[:post_code].should == "OX13 4HG"
  end

  it "uppercases post code letters" do
    data = described_class.new.normalize("sw3 0ta")
    data[:post_code].should == "SW3 0TA"
  end

  it "returns the postcode with an invalid flag if invalid" do
    data = described_class.new.normalize("zzz zzz")
    data.should == {:post_code => "zzz zzz", :invalid => true}
  end

  it "has no outcode if not valid" do
    data = described_class.new.normalize("zzz zzz")
    data[:outcode].should == nil
  end

  it "has the outcode for partial postcodes" do
    data = described_class.new.normalize("W1a")
    data[:post_code].should == "W1A"
    data[:outcode].should == "W1A"
    data[:incode].should be_nil
    data[:invalid].should_not be_true
  end
end
