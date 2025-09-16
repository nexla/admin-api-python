class NotificationsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :notification, required: true
end
