module Chicago
  module ETL
    module SequelExtensions
      module LoadDataInfile
        # Loads the CSV data columns in filepath into this dataset's table.
        def load_csv_infile(filepath, columns)
          execute_dui(load_csv_infile_sql(filepath, columns))
        end
        
        def load_csv_infile_sql(filepath, columns)
          replacement = opts[:insert_ignore] ? "IGNORE" : "REPLACE"
          "LOAD DATA INFILE '#{filepath}' #{replacement} INTO TABLE `#{opts[:from].first}` CHARACTER SET 'utf8' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' (`#{columns.join('`,`')}`);"
        end
      end
    end
  end
end

Sequel::Dataset.send :include, Chicago::ETL::SequelExtensions::LoadDataInfile
