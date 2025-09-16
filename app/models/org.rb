class Org < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::OrgMembership
  include AuditLog
  include Docs
  include SearchableConcern
  include FeaturesConcern
  include MembershipAssociationsTrail
  include ThrottleConcern
  include DataplaneConcern
  include ChangeTrackerConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :billing_owner, class_name: "User", foreign_key: "billing_owner_id"
  belongs_to :org_tier

  # The cluster that an org belongs to defines where itss
  # data flows runs. It may own the cluster itself, or the
  # cluster may be owned by another org, typically Nexla.
  belongs_to :cluster
  belongs_to :new_cluster, class_name: "Cluster", foreign_key: "new_cluster_id"

  # An org may also own multiple clusters. In most environments
  # only the Nexla org will own multiple clusters.
  has_many :clusters, dependent: :destroy

  has_many :org_memberships, dependent: :destroy
  has_many :members, class_name: "User", source: :user, through: :org_memberships
  has_many :api_auth_configs, dependent: :destroy
  has_many :catalog_configs, dependent: :destroy
  has_many :code_containers, dependent: :destroy
  has_many :code_filters, dependent: :destroy
  has_many :custom_data_flows, dependent: :destroy
  has_many :dashboard_transforms, dependent: :destroy
  has_many :data_maps, dependent: :destroy
  has_many :data_schemas, dependent: :destroy
  has_many :data_sinks, dependent: :destroy
  has_many :data_sinks_api_keys, dependent: :destroy
  has_many :data_sets, dependent: :destroy
  has_many :data_sets_api_keys, dependent: :destroy
  has_many :data_sources, dependent: :destroy
  has_many :data_sources_api_keys, dependent: :destroy
  has_many :doc_containers, dependent: :destroy
  has_many :projects, dependent: :destroy
  has_many :quarantine_settings, dependent: :destroy
  has_many :teams, dependent: :destroy
  has_many :data_credentials, class_name: "DataCredentials", dependent: :destroy
  has_many :users_api_keys, dependent: :destroy
  has_many :user_login_audits, -> { order(:created_at => :desc) }, dependent: :destroy
  has_many :flow_nodes
  has_many :domains, dependent: :destroy
  has_many :marketplace_items, dependent: :destroy
  has_many :marketplace_data_sets, -> { merge(MarketplaceItem.active) }, through: :marketplace_items, source: :data_set
  has_many :approval_requests, dependent: :destroy
  has_many :gen_ai_configs, dependent: :destroy
  has_many :flow_templates, dependent: :destroy

  has_many :org_custodians
  has_many :org_custodian_users, through: :org_custodians, source: :user
  has_one :org_additional_info, dependent: :destroy

  has_many :runtimes, dependent: :destroy

  before_save :validate_billing_owner
  before_create :validate_billing_owner
  after_create :handle_after_create

  after_commit do
    ControlService.new(self).publish(:update)
  end

  Nexla_Admin_Email_Domain = "nexla.com"
  Nexla_Admin_Org_Bit = "\x01"

  Statuses = {
    :active => 'ACTIVE',
    :deactivated => 'DEACTIVATED',
    :source_count_capped => 'SOURCE_COUNT_CAPPED',
    :source_data_capped => 'SOURCE_DATA_CAPPED',
    :trial_expired => 'TRIAL_EXPIRED'
  }

  Cluster_Status = {
    :active => 'ACTIVE',
    :migrating => 'MIGRATING'
  }

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

  def self.cluster_status_enum
    enum = "ENUM("
    first = true
    Cluster_Status.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.get_nexla_admin_org
    o = Org.where.not(nexla_admin_org: nil).first
    raise "ERROR: Nexla admin org not found!" if o.nil?
    return o
  end

  def self.validate_status (type_str)
    return nil if type_str.class != String
    return nil if Statuses.find { |sym, str| str == type_str }.nil?
    type_str
  end

  def self.build_from_input (input, api_user, api_org)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    users = input[:users]
    input.delete(:users)

    input[:cluster_id] ||= Cluster.default_cluster.id
    Cluster.validate_cluster_for_org(input[:cluster_id])

    org = nil

    self.transaction do
      owner, set_owner_default_org, ignored =
        get_or_create_org_user(api_user, api_org, input)
      input.delete(:owner)
      input[:owner_id] = owner.id

      billing_owner = nil
      if (input.key?(:billing_owner))
        billing_owner, set_billing_owner_default_org, set_billing_owner_admin =
          get_or_create_org_user(api_user, api_org, input, :billing_owner)
        input.delete(:billing_owner)
        input[:billing_owner_id] = billing_owner.id
      end

      if (input.key?(:org_tier))
        tier = OrgTier.find_by_name(input[:org_tier])
        raise Api::V1::ApiError.new(:bad_request, "Unknown org tier: #{input[:org_tier]}") if tier.nil?
        input.delete(:org_tier)
        input[:org_tier_id] = tier.id
      elsif (input.key?(:org_tier_id))
        tier = OrgTier.find(input[:org_tier_id])
        raise Api::V1::ApiError.new(:bad_request, "Unknown org tier id: #{input[:org_tier_id]}") if tier.nil?
      else
        tier = OrgTier.find_by_name('Free')
        input[:org_tier_id] = tier.id  if !tier.nil?
      end

      input[:referenced_resources_enabled] = true
      org = Org.create(input)
      raise Api::V1::ApiError.new(:bad_request, org.errors.full_messages.join(";")) if !org.valid?

      OrgMembership.create(user: billing_owner, org: org) if !billing_owner.nil?

      if (set_owner_default_org)
        owner.default_org = org
        owner.update_user_notification_settings_org(org)
        owner.save!
      end

      if (set_billing_owner_default_org)
        billing_owner.default_org = org
        billing_owner.save!
      end

      org.add_admin(billing_owner) if (set_billing_owner_admin)
      org.search_index_name = SearchService::OrgSearch.make_index_name(org)
      org.save!

      if org.org_tier.present?
        Org.update_tier_info(org, org.org_tier, nil)
      end
    end

    org.add_users(api_user, api_org, users) if users.is_a?(Array)
    return org
  end

  def add_members (user_or_users)
    self.members << user_or_users
    if user_or_users.is_a?(User)
      user_or_users.reload
    else
      user_or_users.each(&:reload)
    end
  end
  alias_method :add_member, :add_members

  def pending_approval_marketplace_data_sets
    ids = approval_requests.pending.map { |ar| ar.first_step&.result&.fetch(:data_set_id, nil) }.compact
    data_sets.where(id: ids)
  end

  def org
    self
  end

  def login_audits
    user_login_audits.where(audit_type: User::AUTHENTICATION_TYPES[:login])
  end

  def logout_audits
    user_login_audits.where(audit_type: User::AUTHENTICATION_TYPES[:logout])
  end

  def update_mutable! (request, api_user, api_org, input)
    return if !input.is_a?(Hash)

    if input.key?(:cluster_id)
      raise Api::V1::ApiError.new(:bad_request,
        "Cannot set cluster_id in PUT /orgs. Use PUT /orgs/<org-id>/cluster instead.")
    end

    self.transaction do
      self.name = input[:name] if !input[:name].blank?
      self.description = input[:description] if input.key?(:description)
      self.client_identifier = input[:client_identifier] if input.key?(:client_identifier)
      self.email = input[:email] if input.key?(:email)
      self.require_org_admin_to_publish = !!input[:require_org_admin_to_publish] if input.key?(:require_org_admin_to_publish)
      self.require_org_admin_to_subscribe = !!input[:require_org_admin_to_subscribe] if input.key?(:require_org_admin_to_subscribe)
      self.enable_nexla_password_login = !!input[:enable_nexla_password_login] if input.key?(:enable_nexla_password_login)

      if input.key?(:trial_expires_at)
        raise Api::V1::ApiError.new(:forbidden, "You can't change trial expiration date") unless api_user.super_user?
        raise Api::V1::ApiError.new(:bad_request, "Org is not self signup") unless self.self_signup?
        self.extend_trial(input[:trial_expires_at])
      end
      if input.key?(:self_signup_members_limit)
        raise Api::V1::ApiError.new(:forbidden, "You can't change self signup members limit") unless api_user.super_user?
        raise Api::V1::ApiError.new(:bad_request, "Org is not self signup") unless self.self_signup?
        self.self_signup_members_limit = input[:self_signup_members_limit]
      end

      if (input.key?(:owner))
        owner, new_user, ignored =
          Org.get_or_create_org_user(api_user, api_org, input)

        self.owner = owner
        if (new_user)
          owner.default_org = org
          owner.save!
        end
        om = OrgMembership.where(:org => org, :user => owner)
        OrgMembership.create(:org => org, :user => owner) if (om.empty?)
      end

      if (!input[:owner_id].blank?)
        new_owner = User.find(input[:owner_id])
        if (new_owner != self.owner)
          if (!new_owner.org_member?(self))
            raise Api::V1::ApiError.new(:bad_request, "New owner is not a member of the org")
          end
          self.owner = new_owner
        end
      end

      if (input.key?(:billing_owner))
        billing_owner, new_user, set_admin =
          Org.get_or_create_org_user(api_user, api_org, input, :billing_owner)

        self.billing_owner = billing_owner
        if (new_user)
          billing_owner.default_org = org
          billing_owner.save!
        end
        if (set_admin)
          org.add_admin(billing_owner)
        else
          org.remove_admin(billing_owner)
        end
        om = OrgMembership.where(:org => org, :user => billing_owner)
        OrgMembership.create(:org => org, :user => billing_owner) if (om.empty?)
      end

      if (!input[:billing_owner_id].blank?)
        new_billing_owner = User.find(input[:billing_owner_id])
        if (new_billing_owner != self.owner)
          if (!new_billing_owner.org_member?(self))
            raise Api::V1::ApiError.new(:bad_request, "New billing owner is not a member of the org")
          end
          self.billing_owner = new_billing_owner
        end
      end

      prev_tier = self.org_tier
      if (input.key?(:org_tier))
        tier = OrgTier.find_by_name(input[:org_tier])

        raise Api::V1::ApiError.new(:forbidden, "You can't change org tier") unless api_user.super_user?
        raise Api::V1::ApiError.new(:bad_request, "Unknown org tier: #{input[:org_tier]}") if tier.nil?
        self.org_tier_id = tier.id
      elsif (input.key?(:org_tier_id))
        tier = OrgTier.find(input[:org_tier_id])

        raise Api::V1::ApiError.new(:forbidden, "You can't change org tier") unless api_user.super_user?
        raise Api::V1::ApiError.new(:bad_request, "Unknown org tier id: #{input[:org_tier_id]}") if tier.nil?
        self.org_tier_id = tier.id
      end

      self.save!
      Org.update_tier_info(self, self.org_tier, prev_tier)
    end

    # Note, we add users outside the transaction. Not necessary to
    # rollback Org attribute changes if a user addition fails.
    self.add_users(api_user, api_org, input[:users] || input[:members])
    self.update_custodians!(api_user, input[:custodians], :reset)
  end

  def cluster_migrating?
    self.cluster_status == Cluster_Status[:migrating]
  end

  def cluster_active?
    self.cluster_status == Cluster_Status[:active]
  end

  def update_cluster (dst_cluster_id)
    self.transaction do
      self.reload
      if (self.cluster_status == Cluster_Status[:migrating])
        raise Api::V1::ApiError.new(:method_not_allowed,
          "Org is already being migrated to a new cluster")
      end
      if (self.cluster_id != dst_cluster_id)
        Cluster.validate_cluster_for_org(dst_cluster_id, self.id)
        self.new_cluster_id = dst_cluster_id
        self.cluster_status = Cluster_Status[:migrating]
        self.save!
      end
    end
  end

  def revert_cluster
    self.transaction do
      self.cluster_status = Cluster_Status[:active]
      self.new_cluster_id = nil
      self.save!
    end
  end

  def set_cluster_status_active
    self.transaction do
      if !self.new_cluster.present?
        raise Api::V1::ApiError.new(:internal_server_error,
          "No new cluster to activate!")
      end
      self.cluster_id = self.new_cluster_id
      self.new_cluster_id = nil
      self.cluster_status = Cluster_Status[:active]
      self.save!
    end
  end

  def update_members (api_user, api_org, members, mode)
    return if members.blank?
    case mode
    when :add
      add_users(api_user, api_org, members)
    when :remove, :activate, :deactivate
      destroy_members = []
      deactivate_members = []
      activate_members = []
      users_collection = []

      OrgMembership.transaction do
        members.each do |member|
          member.symbolize_keys!
          user = User.find_by("email like ? or id = ?", member[:email], member[:id])
          if !user.present?
            raise Api::V1::ApiError.new(:bad_request, "Cannot find the user with email or id  #{member[:email] || member[:id]}")
          end
          org_membership = org_memberships.find_by(user_id: user.id)
          if !org_membership.present?
            raise Api::V1::ApiError.new(:bad_request, "Cannot find the org membership for user with email or id  #{member[:email] || member[:id]}")
          end
          if mode == :activate
            org_membership.activate!
            activate_members << member
          end
          if mode == :deactivate || mode == :remove
            org_membership.deactivate!(member[:delegate_owner_id], member[:pause_data_flows])
            deactivate_members << member if mode == :deactivate
          end

          if mode == :remove
            destroy_members << member
            org_membership.destroy
          end
          users_collection << org_membership.user
        end
      end

      self.log_members_event(:membership_removed, self, destroy_members, users_collection)
      self.log_members_event(:membership_deactivated, self, deactivate_members, users_collection) if mode == :deactivate
      self.log_members_event(:membership_activated, self, activate_members, users_collection) if mode == :activate
    end
  end

  def add_users (api_user, api_org, users)
    return unless users.is_a?(Array)

    user_entities = []
    updated_users_hashes = []
    added_users_hashes = []

    users.each do |user_hash|
      user_hash.symbolize_keys!
      user_hash[:email]&.downcase!

      id_hash = { email: user_hash[:email], id: user_hash[:id] }.compact
      if user_hash.key?(:admin) && user_hash.key?(:access_role)
        raise Api::V1::ApiError.new(:bad_request, "Providing both 'admin' and 'access_role' attributes is not allowed (provided for user #{id_hash.to_json})")
      end

      user = (User.find_by("email like ?", user_hash[:email]) || User.find_by_id(user_hash[:id]))

      if (user.nil?)
        user_hash[:default_org_id] = self.id

        if self.self_signup? && self.members.count >= self.members_limit
          raise Api::V1::ApiError.new(:bad_request, "Cannot add more members to the org")
        end

        user = User.build_from_input(
          user_hash.except(:access_role, :admin, :access_role_expiration_seconds),
          api_user,
          api_org
        )
      end

      user_hash[:id] = user.id
      user_hash[:email] = user.email

      user.save! unless user.persisted?

      om = OrgMembership.where(org: self, user: user)
      membership_existed = om.present?
      OrgMembership.create(org: org, user: user) unless membership_existed

      # Set user's org context
      user.org = self

      user_entities << user

      # Can't edit role of the owner.
      next if (self.owner_id == user.id)

      role = user_hash[:access_role]
      admin = user_hash[:admin] || role == 'admin'
      role = 'admin' if admin
      role = 'none' if role == 'member'
      role ||= 'none'

      role = role.downcase

      access_role_expires_at = nil
      if user_hash[:access_role_expiration_seconds].present? && (role != 'none')
        expiration_seconds = user_hash[:access_role_expiration_seconds].to_i
        # Note: less than 0 is treated the same as nil: untimed access
        if (expiration_seconds == 0)
          # Caller is revoking timed access. There is a corner case here
          # of passing 0 for a user that currently has untimed access.
          # Going to treat that the same as revoking timed access.
          access_role_expires_at = Time.now - 1.seconds
        elsif (expiration_seconds > 0)
          access_role_expires_at = (Time.now + expiration_seconds.seconds)
        end
      end

      role_was = (self.get_access_role(user) || :member).to_s.downcase

      # for history
      user_hash[:admin] = (role == 'admin')
      user_hash[:access_role] = role
      user_hash[:access_role_expires_at] = access_role_expires_at

      admin_record = self.ac_records(api_user, org)
      if user_hash[:admin]
        if admin_record.any?{|acl| acl.expires_at.present? }
          raise Api::V1::ApiError.new(:forbidden, "Admin with temporary access cannot add another admin")
        end
      end

      if membership_existed
        updated_users_hashes << user_hash if role != role_was
      else
        added_users_hashes << user_hash
      end

      if role == 'none'
        remove_accessors([{ id: user.id, type: :user }])
      else
        add_accessors([
          {
            id: user.id,
            type: :user,
            access_role: role,
            access_role_expires_at: access_role_expires_at
          }
        ])
      end
    end

    self.log_members_event(:membership_added, self, added_users_hashes, user_entities, add_role: true, save_is_admin: true)
    self.log_members_event(:membership_updated, self, updated_users_hashes, user_entities, add_role: true, save_is_admin: true)
  end

  def active?
    (self.status == Statuses[:active])
  end

  def activate!
    if (self.owner.deactivated?)
      raise Api::V1::ApiError.new(:method_not_allowed, "Org cannot be activated while owner's account is deactivated: #{self.owner.id}")
    end
    if (self.owner.account_locked?)
      raise Api::V1::ApiError.new(:method_not_allowed, "Org cannot be activated while owner's account is locked: #{self.owner.id}")
    end
    if (!self.owner.org_member?(self))
      raise Api::V1::ApiError.new(:method_not_allowed, "Org cannot be activated while owner's membership is deactivated: #{self.owner.id}")
    end
    self.status = Statuses[:active]
    self.save!
  end

  def deactivated?
    (self.status == Statuses[:deactivated])
  end

  def deactivate! (pause_flows = false)
    self.status = Statuses[:deactivated]
    self.save!
  end

  def is_nexla_admin_org?
    return self.nexla_admin_org?
  end

  def admin_users
    admin_users = [self.owner]
    self.access_controls.each do |ac|
      next if !ac.enables_role?(:admin)
      accessor = ac.accessor
      if (accessor.is_a?(User))
        admin_users << accessor
      elsif (accessor.is_a?(Team))
        admin_users += accessor.members
      end
    end

    admin_users.uniq(&:id)
  end

  def org_webhook_host
    DataIngestionService.new.get_webhook_host(self)
  end

  def nexset_api_host
    EnvironmentUrl.instance.nexset_api_url(self)
  end

  def sso_options
    # BEWARE this method is called from an unauthenticated route.
    # Be careful about what attributes are revealed.
    return self.api_auth_configs.map(&:public_attributes)
  end

  def handle_after_create
    OrgMembership.create(org: self, user: self.owner)
    update_org_default_notification_settings
  end

  def update_org_default_notification_settings
    notification_type = NotificationType.where(:event_type => 'MONITOR', :resource_type => 'SOURCE').first

    if !notification_type.nil?
      channel = 'EMAIL'
      notification_channel_setting = NotificationChannelSetting.where(
        :channel => channel, :owner_id => self.owner.id, :org_id => self.id).first

      if notification_channel_setting.nil?
        notification_channel_setting = NotificationChannelSetting.new
        notification_channel_setting.set_defaults(self.owner, self)
        notification_channel_setting.channel = channel
        notification_channel_setting.save
      end

      notification_config = {
        :frequency_cron => "0 0 0 1/1 * ? *",
        :monitor_window => 86400000,
        :threshold_value => 90,
        :threshold_type => "PERCENTAGE",
        :threshold_condition => "lessThan"
      }

      notification_setting = NotificationSetting.new
      notification_setting.set_defaults(self.owner, self)
      notification_setting.notification_type_id = notification_type.id
      notification_setting.resource_id = self.id
      notification_setting.notification_resource_type = 'ORG'
      notification_setting.channel = channel
      notification_setting.status = 'ACTIVE'
      notification_setting.notification_channel_setting_id = notification_channel_setting.id
      notification_setting.config = notification_config
      notification_setting.save
    end
  end

  def self.activate_rate_limited_sources!(resource, resource_tier, data_limit_validate = false, tier_updated = false)
    return if (resource.status == Statuses[:trial_expired] && !tier_updated)
    rate_limited_sources = Org.get_data_sources(resource, status: DataSource::Statuses[:rate_limited])
    active_source_count = Org.get_active_source_count(resource)

    data_source_count_limit = resource_tier.nil? ? OrgTier::Unlimited : resource_tier.data_source_count_limit
    OrgTier.activate_rate_limited_sources(rate_limited_sources, data_source_count_limit, active_source_count, resource)

    if data_limit_validate && !resource_tier.nil?
      resource.status = Org.compute_account_status(resource)
      resource.save!
    end
  end

  def self.compute_account_status(resource)
    rate_limited_source_count = Org.get_rate_limited_source_count(resource)
    if rate_limited_source_count > 0
      return Statuses[:source_count_capped]
    else
      return Statuses[:active]
    end
  end

  def self.pause_rate_limited_sources!(resource, resource_tier, status = nil, resource_count = 0, data_limit_validate = false)
    if data_limit_validate && !resource_tier.nil? && !status.nil?
      resource.status = Org.validate_status(status)
      resource.save!
    end

    active_sources = Org.get_data_sources(resource, status: DataSource::Statuses[:active], data_sink: nil)
    action = (!status.nil? && Org.validate_status(status) == Statuses[:trial_expired]) ? "pause" : "rate_limited"
    OrgTier.pause_rate_limited_sources(active_sources, resource_count, action)
  end

  def self.account_expired?(org, user)
    if !org.nil? && org.status == Statuses[:trial_expired]
      return true
    elsif !user.nil? && user.status == Statuses[:trial_expired]
      return true
    end
    return false
  end

  def self.update_tier_info (resource, resource_tier, prev_tier)
    if resource.respond_to?(:email_domain) && resource.email_domain == Org::Nexla_Admin_Email_Domain
      resource.update!(trial_expires_at: nil)
    else
      if resource_tier && resource.respond_to?(:trial_expires_at)
        if resource.trial_expires_at.nil?
          resource.trial_expires_at = resource.created_at + resource_tier.trial_period_days.days
          resource.save!
        elsif prev_tier.trial_period_days && (resource.trial_expires_at == resource.created_at + prev_tier.trial_period_days.days)
          resource.trial_expires_at = resource.created_at + resource_tier.trial_period_days.days
          resource.save!
        end
      end
    end

    if !resource_tier.nil? && !Org.trial_period_expired?(resource, resource_tier)
      if !prev_tier.nil? and prev_tier.id != resource_tier.id
        if resource_tier.data_source_count_limit == OrgTier::Unlimited
          Org.activate_rate_limited_sources!(resource, resource_tier, false, true)
        elsif ((prev_tier.data_source_count_limit == OrgTier::Unlimited && resource_tier.data_source_count_limit != OrgTier::Unlimited) ||
            prev_tier.data_source_count_limit > resource_tier.data_source_count_limit)
          active_source_count = Org.get_active_source_count(resource)
          Org.pause_rate_limited_sources!(resource, resource_tier, nil, active_source_count - resource_tier.data_source_count_limit)
        else
          Org.activate_rate_limited_sources!(resource, resource_tier, false, true)
        end
      elsif prev_tier.nil?
        if resource_tier.data_source_count_limit != OrgTier::Unlimited
          active_source_count = Org.get_active_source_count(resource)
          Org.pause_rate_limited_sources!(resource, resource_tier, nil, active_source_count - resource_tier.data_source_count_limit)
        end
      end

      if !Org.trial_period_expired?(resource, resource_tier)
        resource.status = Org.compute_account_status(resource)
        resource.save!
      end
    end

    if resource.respond_to?(:self_signup) && resource.self_signup? && resource_tier
      unless resource_tier.name.starts_with?('SELF_SIGNUP')
        resource.update(self_signup: false)
      end
    end
  end

  def self.trial_period_expired?(resource, resource_tier)
    return false if resource_tier.nil?
    return false if resource_tier.trial_period_days == OrgTier::Unlimited

    if resource.respond_to?(:trial_expires_at)
      return false if resource.trial_expires_at.nil?
      return resource.trial_expires_at.past?
    else
      return (resource.created_at + resource_tier.trial_period_days.to_i.day).past?
    end
  end

  def self.get_data_sources(resource, filter)
    sources = DataSource.where(filter)
    if resource.is_a?(Org)
      return sources.where(:org_id => resource.id)
    else
      return sources.where(:owner_id => resource.id)
    end
  end

  def self.get_active_source_count(resource)
    return Org.get_data_sources(resource, status: DataSource::Statuses[:active], data_sink: nil).count
  end

  def self.get_rate_limited_source_count(resource)
    return Org.get_data_sources(resource, status: DataSource::Statuses[:rate_limited]).count
  end

  def self.all_member_roles (nexla_org)
    aac = Hash.new
    OrgsAccessControl.find_each do |oac|
      next if oac.accessor_org_id.nil?
      case oac.accessor_type
      when AccessControls::Accessor_Types[:user]
        aac[oac.accessor_id] ||= Hash.new
        aac[oac.accessor_id][oac.accessor_org_id] ||= 999
        if (oac.role_index < aac[oac.accessor_id][oac.accessor_org_id])
          aac[oac.accessor_id][oac.accessor_org_id] = AccessControls::ALL_ROLES_SET[oac.role_index]
        end
      when AccessControls::Accessor_Types[:team]
        # Not handled case
      end
    end
    amr = Hash.new
    oms = OrgMembership.all.jit_preload
    oms.each do |om|
      next if (om.user_id.nil? || om.org_id.nil?)
      is_owner = (om.user_id == om.org&.owner_id)
      amr[om.user_id] ||= Hash.new
      amr[om.user_id][:memberships] ||= Array.new
      if (om.org_id == nexla_org.id && om.active?)
        amr[om.user_id][:super_user] = is_owner || (aac.dig(om.user_id, om.org_id) == :admin)
      end
    end
    oms.each do |om|
      next if (om.user_id.nil? || om.org_id.nil?)
      is_owner = (om.user_id == om.org&.owner_id)

      ar = :admin if !!amr[om.user_id][:super_user]
      ar ||= aac.dig(om.user_id, om.org_id)
      ar ||= (is_owner ? :admin : :member)

      amr[om.user_id][:memberships] << {
        id: om.org_id,
        name: om.org&.name,
        is_admin?: (ar == :admin) || is_owner,
        access_role: ar,
        org_membership_status: om.status
      }
    end
    return amr
  end

  def active_catalog_config
    catalog_configs.find_by(status: 'ACTIVE')
  end

  # expect input of format `[{email: 'user@email.com'}, {id: 123123}]`, both variants are consumed
  def update_custodians!(api_user, custodians, mode = :reset)
    custodians = [] if custodians.nil?

    users = custodians.map do |custodian|
      custodian.symbolize_keys!
      user = User.find_by("email = ? or id = ?", custodian[:email], custodian[:id])
      unless user.present?
        raise Api::V1::ApiError.new(:bad_request, "Cannot find the user with email or id  #{custodian[:email] || custodian[:id]}")
      end
      if mode != :remove && !user.org_member?(self)
        raise Api::V1::ApiError.new(:bad_request, "Only org member can be assigned as custodians")
      end
      unless Ability.new(api_user).can?(:read, user)
        raise Api::V1::ApiError.new(:forbidden, "You don't have an access to user you're trying to assign as custodian")
      end
      user
    end

    Org.transaction do
      case mode
      when :add
        users.each do |user|
          org_custodians.create!(user_id: user.id)
        end
      when :remove
        users.each do |user|
          org_custodians.where(user_id: user.id).destroy_all
        end
      when :reset
        self.org_custodians.destroy_all
        users.each do |user|
          org_custodians.create!(user_id: user.id)
        end
      end
    end

    self.org_custodian_users
  end

  def self.expire_trial!
    excluded_statuses = [Statuses[:trial_expired], Statuses[:deactivated]]

    # only expire self_signup orgs
    scope = Org .where.not(trial_expires_at: nil)
                .where.not(status: excluded_statuses)
                .where(self_signup: true)
                .where('trial_expires_at < ?', Time.now)

    scope.each do |org|
      if Org.trial_period_expired?(org, org.org_tier)
        org.status = Statuses[:trial_expired]
        org.save!

        trial_period_end = org.trial_expires_at
        FlowNode.origins_only.where(org_id: org.id).each(&:flow_pause!)
        NotificationService.new.publish_trial_period_end(org.owner, trial_period_end, org)
        UserEventsWebhooksWorker.perform_async('trial_expired', { user_id: org.owner_id, org_id: org.id }.as_json)
      end
    end
  end

  def members_limit
    (self.self_signup_members_limit || ENV.fetch('API_SELF_SIGNUP_MEMBERS_LIMIT', 2).to_i)
  end

  def self.trial_expires_soon
    excluded_statuses = [Statuses[:trial_expired], Statuses[:deactivated]]
    start_date = Time.current.beginning_of_day + 3.days
    end_date = start_date.end_of_day
    orgs = Org.where(self_signup: true)
              .where(trial_expires_at: start_date..end_date)
              .where.not(status: excluded_statuses)
              .includes(:owner)

    orgs.each do |org|
      NotificationService.new.publish_trial_period_ends_soon(org.owner, org)
    end
  end

  def self.get_or_create_org_user (api_user, api_org, input, key = :owner)
    id_key = (key.to_s + "_id").to_sym
    if (!input.key?(id_key) && !input.key?(key))
      raise Api::V1::ApiError.new(:bad_request, "Org #{key} information missing from input")
    end

    admin = false
    set_default_org = false

    if (!input[id_key].nil?)
      user = User.find(input[id_key])
    else
      input = input[key]

      # Note, use User::find() for id because we want an
      # exception to be raised if the user doesn't exist.
      # Otherwise, if user is specified by email, we
      # support creating the User on the fly.
      user = input.key?("id") ? User.find(input["id"]) :
        User.find_by('email like ?', input["email"])

      if (user.nil?)
        user = User.build_from_input(input, api_user, api_org)
        set_default_org = true
      end

      admin = !!input["admin"]
    end

    return user, set_default_org, admin
  end


  def self.org_users_with_roles(org)
    org_roles = OrgsAccessControl.where(org_id: org.id, accessor_type: Accessible::Accessor_Types[:user])
      .where("expires_at is null or expires_at > ?", Time.now)
      .pluck(:accessor_id, :role_index, :expires_at)

    org_role_indexes = org_roles.map{|role| role.values_at(0, 1).to_a }.to_h
    org_roles_expirations = org_roles.map{|role| role.values_at(0, 2) }.to_h

    org_team_memberships = TeamMembership.preload(:team).joins(:team).where(team: {org: org})
    team_hash = org_team_memberships.pluck(:user_id, :team_id)
                                    .group_by{|a| a[0] }
                                    .to_h
                                    .transform_values{|a| a.map(&:second) }

    team_ids = org_team_memberships.pluck('distinct team_id')
    team_roles = OrgsAccessControl.where(org_id: org.id, accessor_type: Accessible::Accessor_Types[:team], accessor_id: team_ids)
                                          .pluck(:accessor_id, :role_index, :expires_at)
    team_role_indexes = team_roles.map{|role| role.values_at(0, 1) }.to_h
    team_role_expirations = team_roles.map{|role| role.values_at(0, 2) }.to_h

    org_memberships = OrgMembership.where(org: org).joins(:user)
    user_ids = org_memberships.distinct.pluck(:user_id)

    roles_result = {}
    expirations = {}
    roles_result[org.owner_id] = Accessible::Access_Roles.index(:admin)
    user_ids.each do |user_id|
      team_role = team_hash[user_id]&.map{|team_id| team_role_indexes[team_id] }&.compact&.min
      existing = roles_result[user_id]

      roles_result[user_id] = [org_role_indexes[user_id], team_role, existing].compact.min

      expirations[user_id] =  case roles_result[user_id]
                              when org_role_indexes[user_id]
                                org_roles_expirations[user_id]
                              when team_role
                                team_role_expirations[team_hash[user_id]&.first]
                              when existing
                                expirations[user_id]
                              end
    end

    access_roles = roles_result.transform_values{|role_index| (role_index && Accessible::Access_Roles[role_index]) || :member }

    [ org_memberships, access_roles, org_roles_expirations ]
  end

  def self.sync_resources_to_catalog
    current_time = Time.current.utc
    configs = CatalogConfig.joins(:org, :data_credentials)
                           .where(mode: CatalogConfig.modes[:auto],
                                  status: Cluster::Statuses[:active],
                                  schedule_time: current_time.hour,
                                  org: { status: Statuses[:active] },
                                  data_credentials: { connector_type: ConstantResolver.instance.api_connector_types[:data_world] })

    configs.each do |config|
      CatalogWorker::BulkCreateOrUpdate.perform_async(config.id)
    end
  end

  def rate_limit_parent
    org_tier
  end

  protected

  def validate_billing_owner
    self.billing_owner = self.owner if self.billing_owner.nil?
  end

  def extend_trial(expires_at)
    return unless self.self_signup?

    self.trial_expires_at = expires_at
    self.status = Statuses[:active] if self.status == Statuses[:trial_expired]
  end

  def custodian?(user)
    org_custodian_users.include?(user)
  end
end
