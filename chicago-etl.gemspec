# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{chicago-etl}
  s.version = "0.0.5"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = [%q{Roland Swingler}]
  s.date = %q{2012-10-29}
  s.description = %q{ETL tools for Chicago}
  s.email = %q{roland.swingler@gmail.com}
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
    "lib/chicago-etl.rb",
    "lib/chicago/etl.rb",
    "lib/chicago/etl/batch.rb",
    "lib/chicago/etl/buffering_insert_writer.rb",
    "lib/chicago/etl/key_builder.rb",
    "lib/chicago/etl/load_dataset_builder.rb",
    "lib/chicago/etl/mysql_dumpfile.rb",
    "lib/chicago/etl/mysql_load_file_value_transformer.rb",
    "lib/chicago/etl/screens/column_screen.rb",
    "lib/chicago/etl/screens/composite_screen.rb",
    "lib/chicago/etl/screens/invalid_element.rb",
    "lib/chicago/etl/screens/missing_value.rb",
    "lib/chicago/etl/screens/out_of_bounds.rb",
    "lib/chicago/etl/sequel/dependant_tables.rb",
    "lib/chicago/etl/sequel/filter_to_etl_batch.rb",
    "lib/chicago/etl/sequel/load_data_infile.rb",
    "lib/chicago/etl/sink.rb",
    "lib/chicago/etl/table_builder.rb",
    "lib/chicago/etl/task_invocation.rb",
    "lib/chicago/etl/tasks.rb",
    "lib/chicago/etl/transformations/add_etl_batch_id.rb",
    "lib/chicago/etl/transformations/uk_post_code.rb",
    "spec/db_connections.yml.dist",
    "spec/etl/batch_spec.rb",
    "spec/etl/etl_batch_id_dataset_filter.rb",
    "spec/etl/key_builder_spec.rb",
    "spec/etl/load_dataset_builder_spec.rb",
    "spec/etl/mysql_dumpfile_spec.rb",
    "spec/etl/mysql_load_file_value_transformer_spec.rb",
    "spec/etl/screens/composite_screen_spec.rb",
    "spec/etl/screens/invalid_element_spec.rb",
    "spec/etl/screens/missing_value_spec.rb",
    "spec/etl/screens/out_of_bounds_spec.rb",
    "spec/etl/sequel/dependant_tables_spec.rb",
    "spec/etl/sequel/filter_to_etl_batch_spec.rb",
    "spec/etl/sequel/load_data_infile_spec.rb",
    "spec/etl/sink_spec.rb",
    "spec/etl/table_builder_spec.rb",
    "spec/etl/task_spec.rb",
    "spec/etl/transformations/add_batch_id_spec.rb",
    "spec/etl/transformations/uk_post_code_spec.rb",
    "spec/spec_helper.rb"
  ]
  s.homepage = %q{http://github.com/notonthehighstreet/chicago-etl}
  s.licenses = [%q{MIT}]
  s.require_paths = [%q{lib}]
  s.rubygems_version = %q{1.8.6}
  s.summary = %q{Chicago ETL}

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<chicagowarehouse>, [">= 0"])
      s.add_development_dependency(%q<rspec>, ["~> 2"])
      s.add_development_dependency(%q<timecop>, [">= 0"])
      s.add_development_dependency(%q<yard>, [">= 0"])
      s.add_development_dependency(%q<flog>, [">= 0"])
      s.add_development_dependency(%q<jeweler>, [">= 0"])
      s.add_development_dependency(%q<rcov>, [">= 0"])
      s.add_development_dependency(%q<ZenTest>, [">= 0"])
    else
      s.add_dependency(%q<chicagowarehouse>, [">= 0"])
      s.add_dependency(%q<rspec>, ["~> 2"])
      s.add_dependency(%q<timecop>, [">= 0"])
      s.add_dependency(%q<yard>, [">= 0"])
      s.add_dependency(%q<flog>, [">= 0"])
      s.add_dependency(%q<jeweler>, [">= 0"])
      s.add_dependency(%q<rcov>, [">= 0"])
      s.add_dependency(%q<ZenTest>, [">= 0"])
    end
  else
    s.add_dependency(%q<chicagowarehouse>, [">= 0"])
    s.add_dependency(%q<rspec>, ["~> 2"])
    s.add_dependency(%q<timecop>, [">= 0"])
    s.add_dependency(%q<yard>, [">= 0"])
    s.add_dependency(%q<flog>, [">= 0"])
    s.add_dependency(%q<jeweler>, [">= 0"])
    s.add_dependency(%q<rcov>, [">= 0"])
    s.add_dependency(%q<ZenTest>, [">= 0"])
  end
end

