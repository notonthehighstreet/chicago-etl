module Chicago
  module ETL
    # A Stage in the ETL Pipeline.
    #
    # Stage subclasses vary in how they perform their execution - some
    # stages may pipe rows from a source to sinks, others may perform
    # direct in-database updates.
    #
    # @abstract
    class Stage
      # The name of this stage.
      attr_accessor :name

      def initialize(options={})
        @executable = options.has_key?(:executable) ? options[:executable] : true
        @pre_execution_strategies = options[:pre_execution_strategies] || []
      end

      # Returns the unqualified name of this stage.
      def task_name
        raise "This Stage has not been bound to a name" if @name.nil?
        name.name
      end
      
      # Returns true if this stage should be executed.
      def executable?
        @executable
      end

      # Executes this stage in the context of an ETL::Batch.
      #
      # This should not be overridden by subclasses; perform_execution
      # should be changed instead.
      def execute(etl_batch)
        prepare_stage(etl_batch)
        perform_execution(etl_batch)
      end

      protected

      # Does the actual work involved in executing this stage.
      #
      # By default, does nothing. This should be overridden by
      # subclasses.
      def perform_execution(etl_batch)
      end

      private

      def prepare_stage(etl_batch)
        @pre_execution_strategies.each do |strategy| 
          strategy.call(self, etl_batch)
        end
      end
    end
  end
end
