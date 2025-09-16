module AsyncTasks::Tasks
  class Base

    extend Memoist

    attr_reader :task
    def initialize(task)
      @task = task
    end

    def check_preconditions
      raise NotImplementedError
    end

    def explain_arguments
      raise NotImplementedError
    end

    def perform
      setup_paper_trail
      Rails.logger.info("Async task: starting task #{task.id} (#{task.task_type}, args: #{task.arguments})")
      task.start!
      result = run
      task.complete!(result)

      Rails.logger.info("Async task: task #{task.id} is completed. Results: #{task.result}, error: #{task.error}, result_url: #{task.result_url}")
    rescue StandardError => e
      task.error!("#{e.message}\n#{e.backtrace.join("\n")}")
      Rails.logger.error("Async task: execution error: #{e.message}\n#{e.backtrace.join("\n")} ")
      raise e
    end

    def provides_download_link?
      false
    end

    def purge_result
      return unless provides_download_link?
      return unless task.completed?

      file_key = task.result['file_key']
      unless file_key.present?
        Rails.logger.error("Async task: task #{task.id} doesn't have result_url although it has to provide download link")
        return
      end

      result = task.result
      if result['storage'] == 's3'
        S3Service.new.delete_file(result['bucket'], file_key)
      else
        raise Api::V1::ApiError.new(:bad_request, "Unsupported storage type")
      end

      self.task.update(result_purged: true, result_url: nil)
    end

    protected

    def setup_paper_trail
      request_data = task.request_data || {}
      return if request_data.blank?

      paper_trail_data = request_data.symbolize_keys.slice(:request_ip, :request_url, :request_user_agent,
                                                           :user_id, :user_email, :org_id, :impersonator_id)

      PaperTrail.request.controller_info = paper_trail_data
    end

    def result_expires_in
      5.days.to_i
    end

    def report_progress(current, total)
      AsyncTask.where(id: task.id).update_all(progress: current.to_f * 100 / total)
    end

    def run
      raise NotImplementedError
    end

    memoize
    def args
      return {} unless task.arguments.is_a?(Hash)

      (task.arguments || {}).with_indifferent_access
    end

    memoize
    def request_wrapper
      OpenStruct.new(task.request_data || {})
    end

    memoize
    def task_owner
      task.owner.tap do |user|
        user.org = task.org
      end
    end
  end
end