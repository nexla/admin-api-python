class DomainCustodian < ApplicationRecord
  belongs_to :org
  belongs_to :user
  belongs_to :domain

  include AuditLog

  enum status: { active: 'active', deactivated: 'deactivated' }
end
