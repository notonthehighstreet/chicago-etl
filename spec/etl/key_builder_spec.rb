require 'spec_helper'

describe Chicago::ETL::KeyBuilder do
  before :all do
    @schema = Chicago::StarSchema.new
    @schema.define_dimension(:user) do
      columns { integer :original_id }
    end

    @schema.define_dimension(:address) do
      columns do
        string :line1
        string :post_code
      end

      natural_key :line1, :post_code
    end

    @schema.define_dimension(:random) do
      columns do
        string :foo
      end
    end

    @schema.define_dimension(:with_hash) do
      columns do
        binary :hash, :unique => true
      end

      natural_key :hash
    end

    @schema.define_fact(:addresses) do
      dimensions :user, :address
      natural_key :user, :address
    end
  end
  
  before :each do
    @db = stub(:staging_database).as_null_object
    @db.stub(:[]).and_return(stub(:max => nil, :select_hash => {}))
    @sink = stub(:sink).as_null_object
    Chicago::ETL::SchemaTableSinkFactory.stub(:new).
      and_return(stub(:factory, :key_sink => @sink))
  end

  describe "for identifiable dimensions" do
    before :each do
      @dimension = @schema.dimension(:user)
    end

    it "returns an incrementing key, given a row" do
      builder = described_class.for_table(@dimension, @db)
      builder.key(:original_id => 2).first.should == 1
      builder.key(:original_id => 3).first.should == 2
    end

    it "returns the same key for the same record" do
      builder = described_class.for_table(@dimension, @db)
      builder.key(:original_id => 2).first.should == 1
      builder.key(:original_id => 2).first.should == 1
    end

    it "takes into account the current maximum key in the database" do
      @db.stub(:[]).with(:keys_dimension_user).and_return(stub(:max => 2, :select_hash => {}))
      builder = described_class.for_table(@dimension, @db)
      builder.key(:original_id => 1).first.should == 3
    end

    it "returns previously created keys" do
      dataset = stub(:dataset, :max => 1, :select_hash => {40 => 1})
      @db.stub(:[]).with(:keys_dimension_user).and_return(dataset)

      builder = described_class.for_table(@dimension, @db)
      builder.key(:original_id => 30).first.should == 2
      builder.key(:original_id => 40).first.should == 1
    end

    it "raises an error when original_id isn't present in the row" do
      builder = described_class.for_table(@dimension, @db)
      expect { builder.key(:foo => :bar) }.to raise_error(Chicago::ETL::KeyError)
    end
  end

  describe "for non-identifiable dimensions with an existing hash" do
    before :each do
      @builder = described_class.
        for_table(@schema.dimension(:with_hash), @db)
    end

    it "returns an incrementing key, given a row" do
      @builder.key(:hash => "aaa").first.should == 1
      @builder.key(:hash => "aab").first.should == 2
    end

    it "returns the same incrementing key" do
      @builder.key(:hash => "aaa").first.should == 1
      @builder.key(:hash => "aaa").first.should == 1
    end

    it "returns the same incrementing key, ignoring case" do
      @builder.key(:hash => "aaa").first.should == 1
      @builder.key(:hash => "AAA").first.should == 1
    end
  end

  describe "for non-identifiable dimensions with natural keys" do
    before :each do
      @builder = described_class.for_table(@schema.dimension(:address), @db)
    end

    it "returns an incrementing key, given a row" do
      @builder.key(:line1 => "some street", :post_code => "TW3 X45").
        first.should == 1
      @builder.key(:line1 => "some road", :post_code => "TW3 X45").
        first.should == 2
    end

    it "returns the same incrementing key, ignoring case" do
      @builder.key(:line1 => "some street", :post_code => "TW3 X45").
        first.should == 1
      @builder.key(:line1 => "some STREET", :post_code => "TW3 X45").
        first.should == 1
    end

    it "can override default hash preparation" do
      @builder.hash_preparation = lambda {|c| c }

      @builder.key(:line1 => "some street", :post_code => "TW3 X45").
        first.should == 1
      @builder.key(:line1 => "some STREET", :post_code => "TW3 X45").
        first.should == 2
    end

    it "selects the Hex version of the binary column for the cache" do
      dataset = stub(:dataset, :max => 1).as_null_object
      @db.stub(:[]).with(:keys_dimension_address).and_return(dataset)
      @builder = described_class.for_table(@schema.dimension(:address), @db)

      dataset.should_receive(:select_hash).with(:hex.sql_function(:original_id).as(:original_id), :dimension_id).and_return({})
      
      @builder.key(:line1 => "foo")
    end

    it "uses all columns as the natural key if one isn't defined" do
      described_class.
        for_table(@schema.dimension(:random), @db).
        original_key(:foo => "bar").
        should == "3D75EEC709B70A350E143492192A1736"
    end
  end

  describe "for facts" do
    before :each do 
      @builder = described_class.for_table(@schema.fact(:addresses), @db)
    end

    it "increments the id, regardless of row equality" do
      @builder.key({}).first.should == 1
      @builder.key({}).first.should == 2
    end

    it "increments from the last id stored id in the fact table" do
      @db.stub(:[]).with(:facts_addresses).and_return(stub(:max => 100, :select_hash => {}))
      @builder = described_class.for_table(@schema.fact(:addresses), @db)
      @builder.key({}).first.should == 101
    end
  end
end
