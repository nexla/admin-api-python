# frozen_string_literal: true
module TaskRunnerHelper
  class << self
    def run(descriptor)
      logger = SystemLogger.logger(:cron)

      abort("API cron jobs not enabled") unless ApiCronjob.enabled?
      abort("Task descriptor required") unless descriptor.present?
      abort("A block is required") unless block_given?

      job = ApiCronjob.find_by(descriptor: descriptor)
      abort("Unknown API cronjob: #{descriptor}") if job.nil?

      if job.enabled?
        if job.perform?
          yield
        else
          logger.info "CRON (#{Thread.current.object_id}): NOT PERFORMING, already done, #{descriptor}"
        end
      else
        logger.info "CRON (#{Thread.current.object_id}): NOT PERFORMING, not enabled, #{descriptor}"
      end
    end
  end
end
