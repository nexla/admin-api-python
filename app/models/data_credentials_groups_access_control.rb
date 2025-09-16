class DataCredentialsGroupsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :data_credentials_group, required: true
end
