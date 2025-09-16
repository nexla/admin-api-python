class QuarantineSettingsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :quarantine_settings, required: true
end
