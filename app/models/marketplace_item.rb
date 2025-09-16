class MarketplaceItem < ApplicationRecord
  include AuditLog
  include JsonAccessor
  include Api::V1::Schema

  belongs_to :data_set
  belongs_to :org

  has_many :domain_marketplace_items, dependent: :destroy
  has_many :domains, through: :domain_marketplace_items
  has_many :domain_custodian_users, through: :domains

  before_save :set_org_id

  enum status: { draft: 'draft', active: 'active', discontinued: 'discontinued' }

  DEFAULT_MARKETPLACE_ITEM_STATUS = :draft

  json_accessor :data_samples

  def set_org_id
    self.org_id = self.data_set&.org_id
  end

  def delist!
    update!(status: :discontinued)
  end
end
