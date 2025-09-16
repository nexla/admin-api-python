class DomainMarketplaceItem < ApplicationRecord
  include AuditLog

  belongs_to :domain
  belongs_to :marketplace_item

  has_one :data_set, through: :marketplace_item
end
