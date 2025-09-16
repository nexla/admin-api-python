class ServiceKeyEvent < ApplicationRecord
  after_create :limit_service_key_events

  private

  def limit_service_key_events
    begin
      ServiceKeyEventsCleanupWorker.perform_async(self.service_key_id)
    rescue => e
      logger = Rails.configuration.x.error_logger
      logger.error({
        event: "limit_api_key",
        class: "ServiceKeyEventsCleanupWorker",
        id: self.id,
        error: e.message
      }.to_json)
    end
  end

end