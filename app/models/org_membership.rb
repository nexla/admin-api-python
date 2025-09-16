class OrgMembership < ApplicationRecord
  self.primary_key = :id

  belongs_to :user
  belongs_to :org


  Statuses = {
    :active => 'ACTIVE',
    :deactivated => 'DEACTIVATED'
  }

  scope :active, -> { where(status: Statuses[:active]) }

  def self.statuses_enum
    enum = "ENUM("
    first = true
    Statuses.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def active?
    (self.status == Statuses[:active])
  end

  def activate!
    self.status = Statuses[:active]
    self.save!
  end

  def deactivated?
    (self.status == Statuses[:deactivated])
  end

  def deactivate! (delegate_owner_id = nil, pause_data_flows = false)
    self.status = Statuses[:deactivated]
    self.user.pause_flows(self.org) if pause_data_flows.truthy?
    self.save!
    return if !delegate_owner_id.present?

    delegate_owner = User.find_by("email = ? or id = ?", delegate_owner_id, delegate_owner_id)
    if delegate_owner.present? && delegate_owner.org_member?(self.org)
      begin
        TransferUserResourcesWorker.perform_async_with_audit_log(self.user.id, self.org.id, delegate_owner.id)
      rescue => e
        logger = Rails.configuration.x.error_logger
        logger.error({
          event: "Transfer User Resources",
          class: "TransferUserResourcesWorker",
          id: self.id,
          error: e.message
        }.to_json)
        TransferUserResources.transfer(self.user, self.org, delegate_owner)
      end
    end
  end
end