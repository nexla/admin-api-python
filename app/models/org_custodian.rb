class OrgCustodian < ApplicationRecord
  belongs_to :org
  belongs_to :user

  include AuditLog

  enum status: { active: 'active', deactivated: 'deactivated' }
end
