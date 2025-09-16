class OrgsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :org, required: true
end
