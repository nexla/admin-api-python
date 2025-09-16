class ResourceEventNotificationWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'notifications'

  # Note: resource that we want to create notification for may be deleted at this point, so need to prepare sent data
  # before it's actually deleted.
  def perform(org_id, event_data)
    org = Org.find(org_id)
    NotificationService.new.publish_raw(org, event_data)
  rescue StandardError => e
    logger.error("Notification service error: #{e.message}")
  end

  def logger
    @logger ||= Rails.configuration.x.notification_service_logger
  end
end
