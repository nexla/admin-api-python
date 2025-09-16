class DashboardTransformsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :dashboard_transforms, required: true
end
