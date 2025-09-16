class TeamsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :team, required: true
end
