# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "chicago-etl"
  s.version = "0.2.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Roland Swingler"]
  s.date = "2013-11-18"
  s.description = "ETL tools for Chicago"
  s.email = "roland.swingler@gmail.com"
  s.extra_rdoc_files = [
    "LICENSE.txt",
    "README.rdoc"
  ]
  s.files = [
    ".document",
    ".rspec",
    "Gemfile",
    "LICENSE.txt",
    "README.rdoc",
    "Rakefile",
    "VERSION",
    "chicago-etl.gemspec",
    "chicago-flow.gemspec",
    "lib/chicago-etl.rb",
    "lib/chicago/etl.rb",
    "lib/chicago/etl/array_sink.rb",
    "lib/chicago/etl/array_source.rb",
    "lib/chicago/etl/batch.rb",
    "lib/chicago/etl/core_extensions.rb",
    "lib/chicago/etl/counter.rb",
    "lib/chicago/etl/dataset_batch_stage.rb",
    "lib/chicago/etl/dataset_builder.rb",
    "lib/chicago/etl/dataset_source.rb",
    "lib/chicago/etl/errors.rb",
    "lib/chicago/etl/filter.rb",
    "lib/chicago/etl/key_builder.rb",
    "lib/chicago/etl/load_dataset_builder.rb",
    "lib/chicago/etl/mysql.rb",
    "lib/chicago/etl/mysql_file_serializer.rb",
    "lib/chicago/etl/mysql_file_sink.rb",
    "lib/chicago/etl/null_sink.rb",
    "lib/chicago/etl/pipeline.rb",
    "lib/chicago/etl/pipeline_endpoint.rb",
    "lib/chicago/etl/schema_sinks_and_transformations_builder.rb",
    "lib/chicago/etl/schema_table_sink_factory.rb",
    "lib/chicago/etl/screens/column_screen.rb",
    "lib/chicago/etl/screens/invalid_element.rb",
    "lib/chicago/etl/screens/missing_value.rb",
    "lib/chicago/etl/screens/out_of_bounds.rb",
    "lib/chicago/etl/sequel/dependant_tables.rb",
    "lib/chicago/etl/sequel/filter_to_etl_batch.rb",
    "lib/chicago/etl/sink.rb",
    "lib/chicago/etl/stage.rb",
    "lib/chicago/etl/stage_builder.rb",
    "lib/chicago/etl/table_builder.rb",
    "lib/chicago/etl/task_invocation.rb",
    "lib/chicago/etl/tasks.rb",
    "lib/chicago/etl/transformation.rb",
    "lib/chicago/etl/transformation_chain.rb",
    "lib/chicago/etl/transformations.rb",
    "lib/chicago/etl/transformations/deduplicate_rows.rb",
    "lib/chicago/etl/transformations/uk_post_code.rb",
    "lib/chicago/etl/transformations/uk_post_code_field.rb",
    "spec/db_connections.yml.dist",
    "spec/etl/array_sink_spec.rb",
    "spec/etl/array_source_spec.rb",
    "spec/etl/batch_spec.rb",
    "spec/etl/core_extensions_spec.rb",
    "spec/etl/counter_spec.rb",
    "spec/etl/dataset_source_spec.rb",
    "spec/etl/define_dimension_stage_spec.rb",
    "spec/etl/define_stage_spec.rb",
    "spec/etl/etl_batch_id_dataset_filter.rb",
    "spec/etl/filter_spec.rb",
    "spec/etl/key_builder_spec.rb",
    "spec/etl/load_dataset_builder_spec.rb",
    "spec/etl/mysql_file_serializer_spec.rb",
    "spec/etl/mysql_file_sink_spec.rb",
    "spec/etl/mysql_integration_spec.rb",
    "spec/etl/pipeline_stage_builder_spec.rb",
    "spec/etl/schema_table_sink_factory_spec.rb",
    "spec/etl/screens/invalid_element_spec.rb",
    "spec/etl/screens/missing_value_spec.rb",
    "spec/etl/screens/out_of_bounds_spec.rb",
    "spec/etl/sequel/dependant_tables_spec.rb",
    "spec/etl/sequel/filter_to_etl_batch_spec.rb",
    "spec/etl/stage_spec.rb",
    "spec/etl/table_builder_spec.rb",
    "spec/etl/task_spec.rb",
    "spec/etl/transformation_chain_spec.rb",
    "spec/etl/transformation_spec.rb",
    "spec/etl/transformations/deduplicate_rows_spec.rb",
    "spec/etl/transformations/uk_post_code_field_spec.rb",
    "spec/etl/transformations/uk_post_code_spec.rb",
    "spec/etl/transformations_spec.rb",
    "spec/spec_helper.rb"
  ]
  s.homepage = "http://github.com/notonthehighstreet/chicago-etl"
  s.licenses = ["MIT"]
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.25"
  s.summary = "Chicago ETL"

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<chicagowarehouse>, ["~> 0.4"])
      s.add_runtime_dependency(%q<fastercsv>, [">= 0"])
      s.add_runtime_dependency(%q<sequel>, [">= 0"])
      s.add_runtime_dependency(%q<sequel_load_data_infile>, [">= 0.0.2"])
      s.add_runtime_dependency(%q<sequel_fast_columns>, [">= 0"])
      s.add_development_dependency(%q<rspec>, ["~> 2"])
      s.add_development_dependency(%q<timecop>, [">= 0"])
      s.add_development_dependency(%q<yard>, [">= 0"])
      s.add_development_dependency(%q<flog>, [">= 0"])
      s.add_development_dependency(%q<simplecov>, [">= 0"])
      s.add_development_dependency(%q<ZenTest>, [">= 0"])
      s.add_development_dependency(%q<mysql>, ["= 2.8.1"])
      s.add_development_dependency(%q<bundler>, ["~> 1"])
      s.add_development_dependency(%q<jeweler>, [">= 0"])
    else
      s.add_dependency(%q<chicagowarehouse>, ["~> 0.4"])
      s.add_dependency(%q<fastercsv>, [">= 0"])
      s.add_dependency(%q<sequel>, [">= 0"])
      s.add_dependency(%q<sequel_load_data_infile>, [">= 0.0.2"])
      s.add_dependency(%q<sequel_fast_columns>, [">= 0"])
      s.add_dependency(%q<rspec>, ["~> 2"])
      s.add_dependency(%q<timecop>, [">= 0"])
      s.add_dependency(%q<yard>, [">= 0"])
      s.add_dependency(%q<flog>, [">= 0"])
      s.add_dependency(%q<simplecov>, [">= 0"])
      s.add_dependency(%q<ZenTest>, [">= 0"])
      s.add_dependency(%q<mysql>, ["= 2.8.1"])
      s.add_dependency(%q<bundler>, ["~> 1"])
      s.add_dependency(%q<jeweler>, [">= 0"])
    end
  else
    s.add_dependency(%q<chicagowarehouse>, ["~> 0.4"])
    s.add_dependency(%q<fastercsv>, [">= 0"])
    s.add_dependency(%q<sequel>, [">= 0"])
    s.add_dependency(%q<sequel_load_data_infile>, [">= 0.0.2"])
    s.add_dependency(%q<sequel_fast_columns>, [">= 0"])
    s.add_dependency(%q<rspec>, ["~> 2"])
    s.add_dependency(%q<timecop>, [">= 0"])
    s.add_dependency(%q<yard>, [">= 0"])
    s.add_dependency(%q<flog>, [">= 0"])
    s.add_dependency(%q<simplecov>, [">= 0"])
    s.add_dependency(%q<ZenTest>, [">= 0"])
    s.add_dependency(%q<mysql>, ["= 2.8.1"])
    s.add_dependency(%q<bundler>, ["~> 1"])
    s.add_dependency(%q<jeweler>, [">= 0"])
  end
end

