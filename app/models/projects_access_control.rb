class ProjectsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :project, required: true
end
