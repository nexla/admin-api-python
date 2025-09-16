class ServiceKeyEventsCleanupWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'service_account_events'

  def perform(service_account_id)
    begin
      retries ||= 0
      max_events_count = Rails.configuration.x.api["max_api_key_events_count"]
      events = ServiceKeyEvent.where(service_account_id: service_account_id)
      return if events.count <= max_events_count
      events = events.order(created_at: :desc)
      events.where(id: events.offset(max_events_count).ids).delete_all
    rescue => e
      retry if (retries += 1) < 3
    end
  end
end
