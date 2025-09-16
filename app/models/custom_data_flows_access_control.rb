class CustomDataFlowsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :custom_data_flow, required: true
end
