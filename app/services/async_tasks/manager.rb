module AsyncTasks
  class Manager
    ALLOWED_TASK_CLASSES = %w[
      BulkDeleteNotifications BulkMarkAsReadNotifications BulkPauseFlows
      CallProbe ChownUserResources DeactivateUser GetAuditLogs
    ]

    STALE_TASK_TIMEOUT = 1.hour

    RESULT_URL_EXPIRATION = 10.hours

    class << self

      def task_class(task_type)
        validate_task_type!(task_type)
        "AsyncTasks::Tasks::#{task_type}".constantize
      end

      def instantiate_task(task)
        task_class(task.task_type).new(task)
      end

      def instantiate_shallow_task(task_type)
        task_class(task_type).new(AsyncTask.new)
      end

      def validate_preconditions!(task)
        validate_task_type!(task.task_type)
        instantiate_task(task).check_preconditions
      end

      def start_job!(task)
        if task.owner.org.blank?
          task.owner.org = task.org
        end
        validate_preconditions!(task)
        AsyncTasksWorker.perform_async(task.id)
      end

      def results_s3_bucket
        "nx-#{env_name}-async-tasks-results"
      end

      def env_name
        origins = ENV['ALLOWED_ORIGINS']
        origin = origins.presence && origins.split(',').first

        env = origin || 'local-dev'
        return 'dev' if env.include?('test.')
        return 'qa' if env.include?('qa.')
        return 'prod' if env.include?('dataops.')
        return 'beta' if env.include?('beta.')

        'local-dev'
      end

      def result_expire_time
        5.days.to_i
      end

      def purge_expired_results!
        time = result_expire_time.seconds.ago

        AsyncTask.where(status: :completed, result_purged: [nil, false]).where('stopped_at < ?',  time).where.not(result_url: nil).find_each do |task|
          if task.result && task.result['storage'] == 's3'
            S3Service.new.delete_file(task.result['bucket'], task.result['file_key'])
            task.update(result_purged: true, result_url: nil)
          end
        end
      end

      def update_stale_tasks!
        AsyncTask.where(status: :running).where('started_at < ?', STALE_TASK_TIMEOUT.ago).find_each do |task|
          task.update(status: :stale)
        end
      end

      private
      def validate_task_type!(task_type)
        unless ALLOWED_TASK_CLASSES.include?(task_type)
          raise "Invalid task type: #{task_type}. Allowed task types are: #{ALLOWED_TASK_CLASSES.join(', ')}"
        end
      end
    end
  end

end