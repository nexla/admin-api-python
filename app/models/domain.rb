class Domain < ApplicationRecord
  include AuditLog
  include Api::V1::Schema

  belongs_to :org
  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true

  has_many :domain_marketplace_items, dependent: :destroy
  has_many :marketplace_items, through: :domain_marketplace_items
  has_many :data_sets, through: :marketplace_items

  belongs_to :parent, class_name: "Domain", optional: true
  has_many :children, class_name: "Domain", foreign_key: "parent_id", dependent: :nullify

  has_many :domain_custodians, dependent: :destroy
  has_many :domain_custodian_users, through: :domain_custodians, source: :user

  has_many :approval_requests, as: :topic

  MARKETPLACE_ITEMS_COUNT_SQL = <<-SQL
    SELECT count(marketplace_items.id) 
    FROM marketplace_items 
        LEFT JOIN domain_marketplace_items on marketplace_items.id = domain_marketplace_items.marketplace_item_id 
    WHERE domain_marketplace_items.domain_id = domains.id
    AND marketplace_items.status = 'active'
  SQL

  scope :with_items_count, -> { select( "domains.*, ( #{MARKETPLACE_ITEMS_COUNT_SQL} ) as items_count" )  }

  def items_count
    attributes.key?("items_count") ? read_attribute("items_count") : marketplace_items.count
  end

  def self.build_from_input(input, api_user_info)
    domain = Domain.new
    input["org_id"] ||= api_user_info.org.id
    domain.update_mutable!(input, api_user_info)
    domain
  end

  def update_mutable!(input, api_user_info)
    input = input.symbolize_keys
    self.name = input[:name] if input.key?(:name)

    self.description = input[:description] if input.key?(:description)
    self.parent_id = input[:parent_id] if input.key?(:parent_id)

    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org     if (self.org != api_user_info.input_org)

    if self.name.blank? || self.org_id.blank?
      raise Api::V1::ApiError.new(:bad_request, "Domain name and org_id are required")
    end

    ability = Ability.new(self.owner)
    unless ability.can?(:manage, self)
      raise Api::V1::ApiError.new(:forbidden, "Domain owner doesn't have a permission to edit domains for this org")
    end

    self.save!

    self.update_custodians!(api_user_info.user, input[:custodians], :reset)
  end

  def active_custodian_user?(user)
    domain_custodian_users.where(domain_custodians: { status: :active }).include?(user) ||
      self.org.org_custodian_users.where(org_custodians: { status: :active }).include?(user) ||
      self.org.has_admin_access?(user)
  end

  def update_custodians!(api_user, custodians, mode)
    custodians = [] if custodians.nil?

    users = custodians.map do |custodian|
      custodian.symbolize_keys!
      user = User.find_by("email = ? or id = ?", custodian[:email], custodian[:id])
      unless user.present?
        raise Api::V1::ApiError.new(:bad_request, "Cannot find the user with email or id  #{custodian[:email] || custodian[:id]}")
      end
      if mode != :remove && !user.org_member?(self.org)
        raise Api::V1::ApiError.new(:bad_request, "Only domain's org member should be assigned as custodians")
      end
      # We don't have `can :read, User` defined
      # unless Ability.new(api_user).can?(:read, user)
      #   raise Api::V1::ApiError.new(:forbidden, "You don't have an access to user you're trying to assign as custodian")
      # end
      user
    end

    Domain.transaction do
      case mode
      when :add
        users.each do |user|
          domain_custodians.create!(user_id: user.id, org_id: self.org.id)
        end
      when :remove
        users.each do |user|
          domain_custodians.where(user_id: user.id).destroy_all
        end
      when :reset
        self.domain_custodians.destroy_all
        users.each do |user|
          domain_custodians.create!(user_id: user.id, org_id: self.org.id)
        end
      end
    end

    domain_custodian_users
  end

  def requested_marketplace_items_ids
    approval_request_ids = approval_requests.pending.ids
    ApprovalStep.where(approval_request_id: approval_request_ids, step_name: 'FillBasics').pluck(:result).pluck(:data_set_id).compact
  end
end
