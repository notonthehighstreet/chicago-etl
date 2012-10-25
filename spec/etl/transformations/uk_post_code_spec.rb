# -*- coding: utf-8 -*-
require 'spec_helper'

describe Chicago::ETL::Transformations::UkPostCode do
  it "reformats a 6 character postcode" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " SW 30 TA")

    row[:post_code].should == "SW3 0TA"
  end

  it "reformats a 7 character postcode" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " G L515 FD")

    row[:post_code].should == "GL51 5FD"
  end

  it "doesn't reformat a BFPO post code" do
    row, _ = described_class.new(:post_code).
      call(:post_code => "BFPO 23")

    row[:post_code].should == "BFPO 23"
  end

  it "fixes people using 0 instead of O for OX and OL post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " 0X1 4HG")

    row[:post_code].should == "OX1 4HG"

    row, _ = described_class.new(:post_code).
      call(:post_code => " 0L1 4HG")

    row[:post_code].should == "OL1 4HG"
  end

  it "fixes people using 0 instead of O for OX post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " 0X1 4HG")

    row[:post_code].should == "OX1 4HG"
  end

  it "fixes people using 0 instead of O for OL post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " 0L1 0LG")

    row[:post_code].should == "OL1 0LG"
  end

  it "fixes people using 0 instead of O for PO post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " P01 4HG")

    row[:post_code].should == "PO1 4HG"
  end

  it "fixes people using 0 instead of O for SO post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " S01 4HG")

    row[:post_code].should == "SO1 4HG"
  end

  it "fixes people using 0 instead of O for CO post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " C01 4HG")

    row[:post_code].should == "CO1 4HG"
  end

  it "fixes people using 0 instead of O for YO post codes" do
    row, _ = described_class.new(:post_code).
      call(:post_code => " Y01 4HG")

    row[:post_code].should == "YO1 4HG"
  end

  it "fixes shift key slips" do
    row, _ = described_class.new(:post_code).
      call(:post_code => ")X!£ $HG")

    row[:post_code].should == "OX13 4HG"
  end

  it "uppercases post code letters" do
    row, _ = described_class.new(:post_code).
      call(:post_code => "sw3 0ta")

    row[:post_code].should == "SW3 0TA"
  end

  it "can be configured to only run given a block with success" do
    transform = described_class.new(:post_code) {|row| row[:country] == "GB" }

    transform.call(:post_code => "sw1 7yh").first[:post_code].
      should == "sw1 7yh"
    transform.call(:country => "GB", :post_code => "sw1 7yh").
      first[:post_code].should == "SW1 7YH"
  end
end
