class DataCredentialsAccessControl < ApplicationRecord
  self.primary_key = :id
  self.table_name = "data_credentials_access_controls"

  include AuditLog
  include AccessControlUtils

  belongs_to :data_credentials, required: true
end
