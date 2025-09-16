class ApiKeyEvent < ApplicationRecord
  after_create :limit_api_key_events
  
  private
  
  def limit_api_key_events
    begin
      ApiKeyEventsWorker.perform_async(self.api_key_type, self.api_key_id)
    rescue => e
      logger = Rails.configuration.x.error_logger
      logger.error({
        event: "limit_api_key",
        class: "ApiKeyEventsWorker",
        id: self.id,
        error: e.message
      }.to_json)
    end
  end
end