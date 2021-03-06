require 'rake/tasklib'

module Chicago
  module ETL
    # ETL Rake tasks for a Chicago project.
    #
    # To use, simply include:
    #
    #     Chicago::ETL::RakeTasks.new(schema, :staging_db => db)
    #
    # in your project's Rakefile.
    #
    # Provides the following tasks:
    #
    # +db:create_etl_tables+:: defines the tables used for ETL batches
    #                          and the like
    class RakeTasks < Rake::TaskLib
      def initialize(schema, options)
        @schema = schema
        @db = options[:staging_db]

        define
      end

      def define
        namespace :db do
          desc "Creates the etl tables"
          task :create_etl_tables do
            Chicago::ETL::TableBuilder.build(@db)
          end
        end
      end
    end
  end
end
