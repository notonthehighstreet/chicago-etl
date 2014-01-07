require 'spec_helper'

describe "Mysql -> Mysql through transformation chain" do
  let(:dup_row) {
    Class.new(Chicago::ETL::Transformation) {
      def output_streams
        [:default, @options[:onto]].flatten
      end

      def process_row(row)
        new_row = assign_stream(row.dup, @options[:onto])
        [row, new_row]
      end
    }
  }

  before :all do
    unless TEST_DB.table_exists?(:source)
      TEST_DB.create_table(:source) do
        primary_key :id
        varchar :foo
        binary  :bin, :size => 1
      end
    end

    unless TEST_DB.table_exists?(:destination)
      TEST_DB.create_table(:destination) do
        primary_key :id
        varchar :foo
        binary  :bin, :size => 1
      end
    end
  end

  before :each do
    TEST_DB[:source].truncate
    TEST_DB[:destination].truncate
  end

  after :each do
    TEST_DB[:source].truncate
    TEST_DB[:destination].truncate
  end

  it "copies data from source to destination" do
    TEST_DB[:source].multi_insert([{:foo => nil, :bin => :unhex.sql_function("1F")},
                                   {:foo => "Hello", :bin => :unhex.sql_function("1F")}])
    
    source = Chicago::ETL::DatasetSource.
      new(TEST_DB[:source].
          select(:id, :foo, :hex.sql_function(:bin).as(:bin)))

    transformations = [dup_row.new(:onto => :other)]

    sink_1 = Chicago::ETL::MysqlFileSink.
      new(TEST_DB, :destination, [:id, :foo, :bin])
    sink_2 = Chicago::ETL::ArraySink.new([:id, :foo, :bin])

    stage = Chicago::ETL::RowTransformationStage.
      new(:test, 
          :source => source, 
          :transformations => transformations, 
          :sinks => {
            :default => sink_1, 
            :other => sink_2
          })

    stage.execute(double(:etl_batch, :reextracting? => true))

    expected = [{:id => 1, :foo => nil, :bin => "1F"},
                {:id => 2, :foo => "Hello", :bin => "1F"}]

    sink_2.data.should == expected
    TEST_DB[:destination].select(:id, :foo, :hex.sql_function(:bin).as(:bin)).
      all.should == expected
  end
end
