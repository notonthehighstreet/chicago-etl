require 'fileutils'
require 'logger'

module Chicago
  module ETL
    # A particular "run" of the ETL process.
    #
    # All ETL tasks should be executed in the context of a Batch.
    #
    # A batch creates a temporary directory under tmp/batches/:id
    # where it stores various logs and extract files.
    class Batch < Sequel::Model
      set_dataset :etl_batches

      one_to_many :task_invocations

      class << self
        # Returns the Batch that should be used for the ETL process.
        #
        # A new batch is returned, unless the previous batch did not
        # finish successfully.
        #
        # This should be used in preference to new or create.
        def instance
          if last_batch.nil? || last_batch.finished?
            new
          else 
            last_batch
          end
        end
      
        # Returns the last batch run, or nil if this is the first batch.
        def last_batch
          order(:started_at).last
        end
      end

      # Deprecated.
      #
      # @deprecated Use perform_task instead
      def load(task_name, &block)
        perform_task(:load, task_name, &block)
      end

      # Deprecated.
      #
      # @deprecated Use perform_task instead
      def transform(task_name, &block)
        perform_task(:extract, task_name, &block)
      end

      # Marks this batch for re-extraction.
      def reextract
        @reextract = true
        self
      end

      # Returns true when this batch should re-extract - i.e. load
      # records without regard to creation/update times.
      def reextracting?
        !!@reextract
      end

      def extract_from
        return if reextracting?
        value = self.class.dataset.
          where(:state => "Finished").
          where {|r| r.id < id }.
          select(:max.sql_function(:extracted_to).as(:extracted_to)).
          single_value

        if value && value.to_date == Date.today
          (value.to_date - 1).to_time
        else
          value
        end
      end

      # Deprecated.
      #
      # @deprecated Use perform_task instead
      def extract(task_name, &block)
        perform_task(:extract, task_name, &block)
      end

      # Performs a named task if it hasn't already run successfully in
      # this batch.
      def perform_task(stage, task_name, &block)
        task = find_or_create_task_invocation(stage, task_name)
        task.perform(&block) unless task.finished?
      end

      # Returns the directory files & batch logs will be written to.
      def dir
        @dir ||= File.join(Chicago.project_root, "tmp", "batches", id.to_s)
      end

      # Starts this batch.
      def start(extract_to=nil)
        self.extracted_to = extract_to || Date.today
        save
        if !File.exists?(dir)
            FileUtils.mkdir_p(dir, :mode => 0777)
        end

        if state == "Started"
          log.info "Started ETL batch #{id}."
        else
          log.info "Resumed ETL batch #{id}."
        end
        self
      end

      # Finishes this batch, and sets the finished_at timestamp.
      def finish
        update(:state => "Finished", :finished_at => Time.now.utc)
      end

      # Sets this batch to the Error state.
      def error
        update(:state => "Error")
      end

      # Returns true if this batch is finished.
      def finished?
        state == "Finished"
      end

      # Returns true if in the error state
      def in_error?
        state == "Error"
      end

      # Returns the logger for this batch
      def log
        @log ||= Logger.new(File.join(dir, "log"))
      end

      private

      def find_or_create_task_invocation(stage, name)
        attrs = {:stage => stage.to_s.downcase, :name => name.to_s}
        default_attrs = {:stage => stage.to_s.downcase, :name => name.to_s, :attempts => 0.to_s}
        task_invocations_dataset.filter(attrs).first || add_task_invocation(default_attrs)
      end
    end
  end
end
