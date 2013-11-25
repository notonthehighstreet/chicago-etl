require 'spec_helper'

describe "creating and running a dimension stage" do
  let(:rows) { [{:some_field => "value"}] } 
  let(:db) { double(:db).as_null_object }
  let(:schema) { 
    schema = Chicago::StarSchema.new

    schema.define_dimension(:test) do
      columns do
        string :foo
      end
    end

    schema
  }

  let(:pipeline) { Chicago::ETL::Pipeline.new(db, schema)}

  it "glues the source, transformations, and sink correctly" do
    pipeline.define_stage(:load, :dimensions, :test) do
      source do
        db.test_dataset_method
      end
    end

    pipeline.stages.each do |stage|
      stage.execute(double, true)
    end
  end

  it "should set the inserted at time on the dimension"

  it "truncates the dimension if specified"
end
