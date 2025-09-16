class ApiKeyEventsWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'api_key_events'

  def perform api_key_type, api_key_id
    begin
      retries ||= 0
      max_events_count = Rails.configuration.x.api["max_api_key_events_count"]
      events = ApiKeyEvent.where(api_key_type: api_key_type, api_key_id: api_key_id)
      return if events.count <= max_events_count
      events = events.order(created_at: :desc)
      events.where(id: events.offset(max_events_count).ids).delete_all
    rescue => e
      retry if (retries += 1) < 3
    end
  end
end
    