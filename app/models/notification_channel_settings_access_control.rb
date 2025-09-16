class NotificationChannelSettingsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :notification_channel_setting, required: true
end
