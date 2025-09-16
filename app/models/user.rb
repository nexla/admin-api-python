require 'email_validator'

class User < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include Api::V1::Auth
  include AuditLog
  include ThrottleConcern

  acts_as_tagger

  Backend_Admin_Email = "admin@nexla.com".freeze
  Max_Password_Retry_Count = (Rails.configuration.x.api["max_password_retry_count"] || 5)
  Password_Change_Required_After_Days = (Rails.configuration.x.api["password_expires_days"] || 90).days
  Max_Reset_Password_Tries = (Rails.configuration.x.api["max_reset_password_tries"] || 5)
  Reset_Password_Interval_Mins = (Rails.configuration.x.api["reset_password_interval_minutes"] || 1).minutes

  Password_Constraints = { minimum: 8, maximum: 72 }
  Email_Constraints = { minimum: 3, maximum: 254 }
  Extra_Words = [ "nexla", "Nexla", "NEXLA", "test", "Test", "TEST"]

  STRONG_REGEX = /\A(?=.*[a-zA-Z])(?=.*[0-9])(?=.*[\W]).{8,}\z/
  MEDIUM_REGEX = /\A(?=.*[a-zA-Z])(?=.*[0-9]).{8,}\z/
  MIN_PASSWORD_ENTROPY = 16

  # Must contain 8 or more characters
  # Must contain a digit
  # Must contain a lower case character
  # Must contain an upper case character
  # Must contain a symbol
  PASSWORD_FORMAT = /\A
    (?=.{8,})
    (?=.*\d)
    (?=.*[a-z])
    (?=.*[A-Z])
    (?=.*[[:^alnum:]])
  /x

  has_secure_password

  validate :password_weak?, on: [:create, :update]

  validates :password,
            length: Password_Constraints,
            format: { with: PASSWORD_FORMAT, message: "must contain at least 8 characters, one digit, one lower case character, one upper case character, and one symbol" },
            if: :password_changed?

  validates :email,
    :presence => true,
    :uniqueness => { case_sensitive: false },
    :length => Email_Constraints,
    :email => { strict_mode: true },
    :if => :email_changed?

  belongs_to :default_org, class_name: "Org", foreign_key: "default_org_id"
  belongs_to :user_tier

  has_many :team_memberships, dependent: :destroy

  has_many :member_teams, through: :team_memberships, source: :team
  has_many :user_login_audits, -> { order(:created_at => :desc) }, dependent: :destroy
  has_many :api_key_events, -> { order(:created_at => :desc) }, foreign_key: "owner_id", dependent: :destroy

  has_many :org_memberships, dependent: :destroy
  has_many :member_orgs, through: :org_memberships, source: :org

  has_many :user_settings, inverse_of: :owner, foreign_key: "owner_id"
  has_many :users_api_keys, dependent: :destroy

  has_many :org_custodians, dependent: :destroy
  has_many :custodian_for_orgs, through: :org_custodians, source: :org

  has_many :domain_custodians, dependent: :destroy
  has_many :custodian_for_domains, through: :domain_custodians, source: :domain
  has_many :custodian_of_marketplace_items, through: :custodian_for_domains, source: :marketplace_items

  has_many :notification_channel_settings, dependent: :destroy, inverse_of: :owner, foreign_key: "owner_id"

  alias_method :api_keys, :users_api_keys

  # We cache super-user status here to avoid
  # multiple access-controls lookups on same user
  attr_accessor :is_super_user

  # We cache the actual super-user object here
  # in the case of impersonation by a super-user.
  attr_accessor :super_user

  attr_accessor :impersonator
  attr_reader :org

  attr_accessor :suppress_event_notifications
  attr_accessor :infrastructure_user

  after_initialize do
    self.org = nil
    self.super_user = nil
    self.impersonator = nil
    self.is_super_user = nil
    self.infrastructure_user = nil
  end

  after_create :handle_after_create

  before_validation -> { self.email = self.email.downcase if self.email.present? }

  Statuses = {
    :active => 'ACTIVE',
    :deactivated => 'DEACTIVATED',
    :source_count_capped => 'SOURCE_COUNT_CAPPED',
    :source_data_capped => 'SOURCE_DATA_CAPPED',
    :trial_expired => 'TRIAL_EXPIRED'
  }

  AUTHENTICATION_TYPES = {
    login: "login",
    logout: "logout"
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

  Uppers = ('A'..'Z').to_a
  Specials = ['!', '&', '-', '#', '$', '@', '+', '*']
  Nums = (0..9).to_a
  def self.create_temporary_password
    tmp = rand(36**8).to_s(36) + rand(36**8).to_s(36) + rand(36**8).to_s(36)
    tmp.insert(rand(0..tmp.length), Uppers.sample)
    tmp.insert(rand(0..tmp.length), Uppers.sample)
    tmp.insert(rand(0..tmp.length), Specials.sample)
    tmp.insert(rand(0..tmp.length), Specials.sample)
    tmp.insert(rand(0..tmp.length), Nums.sample.to_s)
    tmp
  end

  def self.build_from_input (input, user, org)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    org_id = input[:default_org_id]
    input.delete(:default_org_id)

    admin = input[:admin]
    input.delete(:admin)

    default_org = Org.find(org_id) if !org_id.nil?
    if (!default_org.nil? && !Ability.new(user).can?(:manage, default_org))
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to org")
    end

    if (!input.key?(:password))
      tmp_pwd = User.create_temporary_password
      input[:password] = input[:password_confirmation] = tmp_pwd
      input[:password_change_required_at] = Time.now
    end
    input[:default_org_id] = default_org.id if !default_org.nil?

    if (input.key?(:user_tier))
      tier = UserTier.find_by_name(input[:user_tier])
      raise Api::V1::ApiError.new(:bad_request, "Unknown user tier: #{input[:user_tier]}") if tier.nil?
      input.delete(:user_tier)
      input[:user_tier_id] = tier.id
    elsif (input.key?(:user_tier_id))
      tier = UserTier.find_by_id(input[:user_tier_id])
      raise Api::V1::ApiError.new(:bad_request, "Unknown user tier id: #{input[:user_tier_id]}") if tier.nil?
    end

    input[:email_verified_at] = Time.now.utc if email_verified?(input[:email])
    u = User.create(input)
    if (!u.valid?)
      if (u.errors.messages.key?(:email))
        msg = "Email " + u.errors.messages[:email][0]
      else
        msg = u.errors.full_messages.join(";")
      end
      raise Api::V1::ApiError.new(:bad_request, msg)
    end

    OrgMembership.create(user: u, org: default_org) if (!default_org.nil?)
    u.org = default_org
    u.update_admin_status(user, org, { :admin => admin })
    u.reload
    manage_external_dataset_sharer(u)
    return u
  end

  def self.email_verified?(email)
    return false if email.blank?
    return false if (email.to_s.include?("nexla") && email.to_s.include?("test"))
    return true
  end

  def self.find_user_and_org_by_api_key (api_key, scopes = nil)
    q = { api_key: api_key }
    q["scope"] = scopes if scopes.present?
    user_api_key = UsersApiKey.find_by(q)
    return [nil, nil, nil] if (user_api_key.nil? || !user_api_key.active?)
    return [nil, nil, nil] if !UsersApiKey::Scopes.values.include?(user_api_key.scope)
    return [user_api_key.user, nil, user_api_key] if user_api_key.org.nil?

    om = OrgMembership.where(:user => user_api_key.user, :org => user_api_key.org).first
    return [nil, nil, nil] if (om.nil? || om.deactivated?)
    return [user_api_key.user, user_api_key.org, user_api_key]
  end

  def self.find_user_and_org_by_service_key(api_key)
    service_key = ServiceKey.find_by(api_key: api_key)
    return [nil, nil, nil] unless service_key&.active?

    return [service_key.user, nil, service_key] if service_key.org.nil?

    om = OrgMembership.where(user: service_key.user, org: service_key.org).first
    return [nil, nil, nil] if (om.nil? || om.deactivated?)
    return [service_key.user, service_key.org, service_key]
  end

  def self.verify_password_reset_token (token)
    return nil if token.blank?
    u = PasswordResetToken.verify(token, logger)
    return nil if u.nil?
    return nil if (u.password_reset_token != token)
    raise Api::V1::ApiError.new(:bad_request, "Account Locked", "ACCOUNT_LOCK") if u.account_locked?
    return u
  end

  def self.find_external_idp_user (api_auth_config, email, full_name)
    return [nil, nil] if (email.blank? || api_auth_config.nil?)

    org = api_auth_config.org
    return [nil, nil] if org.nil?

    user = User.find_by_email(email)

    # global mappings aren't bound to a single org
    if api_auth_config.global and !user.nil?
      org = user.default_org
    end

    return [user, org] if !user.nil? && user.org_member?(org)

    # If we get here, either the user doesn't exist or she isn't
    # a member of the org. If auto-create is off, we're done.
    return [nil, nil] if !api_auth_config.auto_create_users_enabled

    if (user.nil?)
      pwd = User.create_temporary_password
      user = User.create({
        :email => email,
        :full_name => full_name,
        :default_org => org,
        :password => pwd
      })
      org.add_members(user)
      org.reload
    else
      org.add_members(user)
      org.reload
    end

    return [user, org]
  end

  def update_mutable! (request, user, org, input)
    return if !user.is_a?(User) || !input.is_a?(Hash)

    self.transaction do
      self.full_name = input[:full_name] if (input.key?(:full_name) and !input[:full_name].blank?)
      self.tos_signed_at = Time.now.utc if (input.key?(:tos_signed_at))
      prev_tier = self.user_tier
      if (input.key?(:user_tier))
        tier = UserTier.find_by_name(input[:user_tier])
        raise Api::V1::ApiError.new(:bad_request, "Unknown user tier: #{input[:user_tier]}") if tier.nil?
        input.delete(:user_tier)
        self.user_tier_id = tier.id
      elsif (input.key?(:user_tier_id))
        tier = UserTier.find_by_id(input[:user_tier_id])
        raise Api::V1::ApiError.new(:bad_request, "Unknown user tier id: #{input[:user_tier_id]}") if tier.nil?
        self.user_tier_id = tier.id
      end

      if (input.key?(:email_verified_at))
        self.email_verified_at = input[:email_verified_at].blank? ? nil : Time.now.utc
      end

      if (!input.key?(:default_org_id).blank?)
        default_org = Org.find_by_id(input[:default_org_id].to_i)
        if (default_org.nil?)
          raise Api::V1::ApiError.new(:bad_request, "Unknown org")
        end
        if (!self.org_member?(default_org))
          raise Api::V1::ApiError.new(:bad_request, "User is not a member of org #{default_org.id}")
        end
        self.default_org = default_org
      end

      self.save!
      Org.update_tier_info(self, self.user_tier, prev_tier)

      if (!input[:password].nil? and !input[:password_confirmation].nil?)
        if input[:password_current].nil?
          raise Api::V1::ApiError.new(:bad_request, "Current password required")
        end
        if !(self.authenticate(input[:password_current]))
          raise Api::V1::ApiError.new(:bad_request, "Invalid current password")
        else
          change_password(input[:password], input[:password_confirmation])
        end
      end

      self.update_admin_status(user, org, input)
    end
  end

  def build_api_key_from_input (api_user_info, input)
    api_key = UsersApiKey.new
    api_key.owner = api_user_info.input_owner
    api_key.user_id = api_user_info.input_owner.id
    api_key.org_id = (api_user_info.input_org.nil? ? nil : api_user_info.input_org.id)
    api_key.update_mutable!(api_user_info, input)
    return api_key
  end

  def update_admin_status (user, org, input)
    return if !input.is_a?(Hash) || !input.key?(:admin)
    admin = input[:admin]

    if ((admin == "*" || admin == true) && !self.default_org.nil?)
      self.org = self.default_org
      self.default_org.add_admin(self) if !self.default_org.has_admin_access?(self)
    elsif (admin.is_a?(Array))
      admin.each do |org_spec|
        next if !org_spec.is_a?(Hash)
        next if !org_spec.key?("admin")

        o = Org.find_by_id(org_spec["org_id"]) if org_spec.key?("org_id")
        raise Api::V1::ApiError.new(:not_found, "Org not found") if o.nil?
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to org") if (!Ability.new(user).can?(:manage, o))
        raise Api::V1::ApiError.new(:forbidden, "User is not a member of org") if !self.org_member?(o)

        # Set org context for the following operations...
        self.org = o

        if (!!org_spec["admin"])
          o.add_admin(self) if !o.has_admin_access?(self)
        else
          o.remove_admin(self)
        end
      end
    end
  end

  def login_audits
    user_login_audits.where(audit_type: AUTHENTICATION_TYPES[:login])
  end

  def logout_audits
    user_login_audits.where(audit_type: AUTHENTICATION_TYPES[:logout])
  end

  def create_password_reset_token(org, origin = nil, force: false, send_email: true)
    now = Time.now.utc

    if force || (self.password_reset_token_at.nil? || (self.password_reset_token_at < (now - Reset_Password_Interval_Mins)))
      self.password_reset_token_at = now
      self.password_reset_token_count = 0
    end

    self.password_reset_token_count = 0 if force
    self.password_reset_token_count += 1
    if (self.password_reset_token_count > Max_Reset_Password_Tries)
      self.account_locked_at = now
      self.save!
      return
    end

    token = PasswordResetToken.create(self)
    return if token.blank? or origin.nil?

    self.password_reset_token = token
    self.password_reset_token_at = now
    self.save!

    if send_email
      NotificationService.new.publish_reset_password(self, org, origin)
    end
  end

  def authenticate_with_previous (new_password)
    result = true
    1.times do
      break if self.authenticate(new_password)
      break if !self.password_digest_1.blank? && BCrypt::Password.new(self.password_digest_1) == new_password
      break if !self.password_digest_2.blank? && BCrypt::Password.new(self.password_digest_2) == new_password
      break if !self.password_digest_3.blank? && BCrypt::Password.new(self.password_digest_3) == new_password
      break if !self.password_digest_4.blank? && BCrypt::Password.new(self.password_digest_4) == new_password
      result = false
    end
    return result
  end

  def change_password (new_password, new_password_confirm)
    if (self.authenticate_with_previous(new_password))
      raise Api::V1::ApiError.new(:bad_request, "Cannot reuse a recent password", "RECENT_PASSWORD")
    end
    User.transaction do
      save_password_digest = self.password_digest
      self.password = new_password
      self.password_confirmation = new_password_confirm
      self.password_digest_4 = self.password_digest_3
      self.password_digest_3 = self.password_digest_2
      self.password_digest_2 = self.password_digest_1
      self.password_digest_1 = save_password_digest
      self.password_change_required_at = Time.now.utc + Password_Change_Required_After_Days
      self.password_reset_token = nil
      self.password_reset_token_at = nil
      self.password_reset_token_count = 0
      self.save!
    end
  end

  def reset_password_retry_count
    if (self.password_retry_count != 0)
      self.password_retry_count = 0
      self.save!
    end
  end

  def increment_password_retry_count
    self.password_retry_count += 1
    self.save!
    # Note, there can be a double save here when password
    # retry count exceeds the max, but it's rare and it
    # it keeps the logic simple.
    self.lock_account if self.password_retry_count_exceeded?
  end

  def password_retry_count_exceeded?
    (self.password_retry_count >= Max_Password_Retry_Count)
  end

  def password_signature
    # We embed this signature in JWT access tokens to
    # allow invalidating tokens issued with prior
    # passwords after password-reset. self.password_digest
    # should never be nil, but in case it is, use the hexdigest
    # of nil.to_s as a placeholder.
    Digest::MD5.hexdigest(self.password_digest.nil? ? nil.to_s : self.password_digest)
  end

  def password_change_required?
    # NEX-4739 Temporarily ignore the password-change
    # requirement until the change flow is improved.
    # !self.password_change_required_at.nil? && (self.password_change_required_at < Time.now)
    return false
  end

  def account_locked?
    !self.account_locked_at.nil?
  end

  def lock_account
    # NEX-9859 don't allow locking infrastructure account, ever.
    # auth.rb will handle not allowing further password
    # attempts until password_retry_count is reset, but
    # the account itself must remain active.
    if self.infrastructure_user?
      raise Api::V1::ApiError.new(:method_not_allowed)
    else
      self.account_locked_at = Time.now.utc
      self.save!
    end
  end

  def unlock_account
    self.password_retry_count = 0
    self.account_locked_at = nil
    self.save!
  end

  def password_changed?
    !self.password.nil?
  end

  def team_member? (t)
    self.teams(t.org, access_role: :member).include?(t)
  end

  def active?
    return (self.status == Statuses[:active])
  end

  def activate! (org = nil)
    self.status = Statuses[:active]
    self.save!

    if !org.nil?
      om = OrgMembership.where(:user_id => self.id, :org_id => org.id)
      om.first.activate! if !om.empty?
    end
  end

  def deactivated?
    return (self.status == Statuses[:deactivated])
  end

  def deactivate! (org = nil, pause_data_flows = false)
    if org.nil?
      # NOTE not including a specific org in the call
      # deactivates the user for all Nexla contexts.

      orgs = Org.where(:owner_id => self.id).where.not(:status => Statuses[:deactivated])
      single_user = true

      orgs.each do |o|
        if (o.members.size > 1)
          single_user = false
          break
        end
      end

      if (!single_user)
        msg = "User cannot be deactivated while owning the following active multi-user orgs: #{orgs.map(&:id)}"
        raise Api::V1::ApiError.new(:method_not_allowed, msg)
      end

      orgs.each(&:deactivate!)
      self.status = Statuses[:deactivated]
      self.save!
    else
      # In this case we're only deactivating the
      # user in a specific Org context.
      om = OrgMembership.where(:user_id => self.id, :org_id => org.id).first
      raise Api::V1::ApiError.new(:not_found, "Org not found") if om.nil?

      if (org.owner.id == self.id)
        if (org.members.size > 1 && !org.deactivated?)
          msg = "User cannot be deactivated while owning the following active multi-user org: #{org.id}"
          raise Api::V1::ApiError.new(:method_not_allowed, msg)
        end
        org.deactivate!
      end

      om.deactivate!
    end
    self.pause_flows(org) if pause_data_flows.truthy?
  end

  def pause_flows (org = nil)
    self.origin_nodes(org, access_role: :owner).each(&:flow_pause!)
  end

  def org=(new_org)
    # When setting the transient org context attribute,
    # we clear the is_super_user attribute so that it
    # can be recalcuated. super_user depends on being
    # in the Nexla admin org context.
    # See NEX-13559 for additional details.
    self.is_super_user = nil
    @org = new_org
  end

  def super_user?
    # Super user privileges are granted to "admin@nexla.com" and
    # to any Nexla org member who has admin privileges on Nexla.
    # NOTE, we can't call nexla_o.has_admin_access?() here because
    # it leads to an infinite recursion.
    # Also, we cache the result in the User instance because
    # repeated calls to .super_user? get expensive.
    if self.is_super_user.nil?
      raise Api::V1::ApiError.new(:internal_server_error, "Org context is not set") if self.org.nil?
      nexla_o = Org.get_nexla_admin_org
      if (self.org&.id != nexla_o.id)
        self.is_super_user = false
      elsif nexla_o.is_owner?(self)
        self.is_super_user = true
      elsif !self.active_org_member?(nexla_o)
        self.is_super_user = false
      else
        self.is_super_user = nexla_o.has_role?(self, :admin, nexla_o)
      end
    end
    is_super_user
  end

  def super_user_read_only?
    nexla = Org.get_nexla_admin_org
    return false if (self.org&.id != nexla.id)
    return false unless self.active_org_member?(nexla)

    return true if nexla.is_owner?(self)

    nexla.has_role?(self, :admin_readonly, nexla)
  end

  def impersonated?
    !self.impersonator.nil?
  end

  def nexla_backend_admin?
    return (self.email == Backend_Admin_Email)
  end

  def self.nexla_backend_admin
    user = User.find_by_email(Backend_Admin_Email)
    user.org = Org.get_nexla_admin_org if !user.nil?
    user
  end

  def infrastructure_user?
    return true if self.nexla_backend_admin?
    !!self.infrastructure_user
  end

  def infrastructure_or_super_user?
    self.super_user? || self.infrastructure_user?
  end

  def account_status (api_org)
    org = api_org.nil? ? self.default_org : api_org
    if (!org.nil? && !org.org_tier.nil?)
      return org.status
    else
      return self.status
    end
  end

  def has_admin_access? (user)
    return false if user.nil?
    return true if (user.id == self.id) || user.super_user?
    user.orgs(:admin).each do |o|
      return true if self.org_member?(o)
    end
    false
  end

  def default_org
    (super || self.member_orgs[0])
  end

  def all_org_memberships
    # Return all the user's org memberships
    # regardless of membership status.
    OrgMembership.where(:user_id => self.id)
  end

  def active_org_member? (o)
    OrgMembership.where(
      :user_id => self.id,
      :org_id => o.id,
      :status => OrgMembership::Statuses[:active]
    ).any?
  end

  def org_member? (o)
    # NOTE do NOT call self.member_orgs.reload here! It has
    # serious performance impacts in production. Just make
    # sure you're calling org.add_members() instead of using
    # org.members << user. add_members() will call the reload
    # for you, without impacting lookup caching.
    self.orgs(:member).include?(o)
  end

  def orgs (access_role = :member)
    case access_role
    when :all
      self.super_user? ? Org.all : self.member_orgs.union(Org.accessible(self, :all))
    when :member
      self.member_orgs
    else
      Org.accessible(self, access_role)
    end
  end

  def active_member_orgs
    self.org_memberships
        .joins(:org)
        .where(status: OrgMembership::Statuses[:active])
        .where(orgs: { status: Org::Statuses[:active] })
        .map(&:org)
  end

  def sso_options
    # BEWARE this method is called from an unauthenticated route.
    # Be careful about what attributes are revealed.
    return [] if self.org_memberships.empty?
    org = (self.default_org || self.org_memberships.first)
    return org.api_auth_configs.map(&:public_attributes)
  end

  def get_api_key (o = nil)
    if (o.is_a?(Integer) )
      return self.api_keys.where(:org_id => o).first
    elsif (o.is_a?(Org))
      return self.api_keys.where(:org_id => o.id).first
    else
      return self.api_keys.where(:org_id => nil).first
    end
  end

  def generate_password_reset_token(new_user = false)
    token = PasswordResetToken.create(self, new_user)
    return if token.blank?

    self.password_reset_token = token
    self.password_reset_token_at = Time.now.utc
    self.save!
  end

  def self.manage_external_dataset_sharer(user)
    sharers = ExternalSharer.where(:email => user.email)
    sharers.each do |sharer|
      sharer_config = {
        :email => user.email,
        :org_id => user.default_org&.id,
        :description => sharer.description,
        :name => sharer.name
      }
      sharer.data_set.update_sharers(sharer_config, :add, user.default_org || user.orgs.first)
      sharer.destroy
    end
  end

  def flow_nodes (org, options = { access_role: :all })
    FlowNode.accessible_by_user(self, org, options)
  end

  def origin_nodes (org, options = { access_role: :all })
    # NOTE most_recent_limit is a workaround for environments where
    # the total flow count visible to the caller is in the thousands.
    # See NEX-10613. This is happening for Clearwater Analytics in
    # particular, in their staging environment.
    #
    # REMOVE this workaround once UI supports pagination on flows
    # list views.
    if options[:most_recent_limit].present?
      oids = self.data_sources(org, { access_role: options[:access_role] })
        .order(updated_at: :desc)
        .limit(options[:most_recent_limit])
        .pluck(:origin_node_id)
      return FlowNode.where(id: oids)
    end
    self.flow_nodes(org, options).where("flow_nodes.id = flow_nodes.origin_node_id and data_source_id is not null or shared_origin_node_id is not null")
  end

  def custom_data_flows (access_role = :all, org = nil, base_scope = nil)
    CustomDataFlow.accessible(self, access_role, org, base_scope)
  end

  def data_sources (org, options = { access_role: :all })
    if options[:most_recent_limit].present?
      options[:selected_ids] = self.data_sources(org, { access_role: options[:access_role] })
        .order(updated_at: :desc)
        .limit(options[:most_recent_limit])
        .pluck(:id)
    end
    DataSource.accessible_by_user(self, org, options)
  end

  def data_sets (org, options = { access_role: :all })
    if (options[:access_role] == :sharer)
      DataSet.accessible(self, :sharer, org)
    else
      if options[:most_recent_limit].present?
        options[:selected_ids] = self.data_sets(org, { access_role: options[:access_role] })
          .order(updated_at: :desc)
          .limit(options[:most_recent_limit])
          .pluck(:id)
      end
      DataSet.accessible_by_user(self, org, options)
    end
  end

  def data_schemas (org = nil, options = {access_role: :all})
    DataSchema.accessible_by_user(self, org, options)
  end

  def data_sinks (org, options = { access_role: :all })
    if options[:most_recent_limit].present?
      options[:selected_ids] = self.data_sinks(org, { access_role: options[:access_role] })
        .order(updated_at: :desc)
        .limit(options[:most_recent_limit])
        .pluck(:id)
    end
    DataSink.accessible_by_user(self, org, options)
  end

  def data_maps (org, options = { access_role: :all })
    DataMap.accessible_by_user(self, org, options)
  end

  def data_credentials (org, options = { access_role: :all })
    DataCredentials.accessible_by_user(self, org, options)
  end

  def data_credentials_groups (org, options = { access_role: :all })
    DataCredentialsGroup.accessible_by_user(self, org, options)
  end

  def notifications (access_role = :all, org = nil)
    Notification.accessible(self, access_role, org)
  end

  def notification_channel_settings (access_role = :all, org = nil)
    NotificationChannelSetting.accessible(self, access_role, org)
  end

  def notification_settings (access_role = :all, org = nil)
    NotificationSetting.accessible(self, access_role, org)
  end

  def quarantine_settings (access_role = :all, org = nil)
    QuarantineSetting.accessible(self, access_role, org)
  end

  def dashboard_transforms (access_role = :all, org = nil)
    DashboardTransform.accessible(self, access_role, org)
  end

  def transforms (org, options = { access_role: :all })
    Transform.accessible_by_user(self, org, options)
      .where(:output_type => CodeContainer::Output_Types[:record],
        :resource_type => CodeContainer::Resource_Types[:transform])
  end

  def attribute_transforms (org, options = { access_role: :all })
    AttributeTransform.accessible_by_user(self, org, options)
      .where(:output_type => CodeContainer::Output_Types[:attribute],
        :resource_type => CodeContainer::Resource_Types[:transform])
  end

  def validators (org, options = { access_role: :all })
    Validator.accessible_by_user(self, org, options)
      .where(:output_type => CodeContainer::Output_Types[:record],
        :resource_type => CodeContainer::Resource_Types[:validator])
  end

  def error_transforms (access_role = :all, org = nil)
    CodeContainer.accessible(self, access_role, org).where(:resource_type => CodeContainer::Resource_Types[:error])
  end

  def code_containers (org,  options = { access_role: :all })
    CodeContainer.accessible_by_user(self, org, options)
  end

  def doc_containers (access_role = :all, org = nil, base_scope = nil)
    DocContainer.accessible(self, access_role, org, base_scope)
  end

  def projects (org, options = { access_role: :all })
    Project.accessible_by_user(self, org, options)
  end

  def teams (org, options = { access_role: :member })
    if (org == :all)
      org_ids = self.orgs(:member).pluck(:id)
      return self.member_teams.where('teams.org_id in (?) or teams.org_id is NULL', org_ids)
    end

    case options[:access_role]
    when :all
      self.member_teams.where(:org => org).union(Team.accessible_by_user(self, org, options))
    when :member
      self.member_teams.where(:org => org)
    else
      Team.accessible_by_user(self, org, options)
    end
  end

  def users (access_role = :all, org = nil)
    if (self.super_user? && (access_role == :all))
      users = User.all.jit_preload
    elsif (!org.nil? && org.has_admin_access?(self) && access_role == :all)
      users = org.members
    else
      users = User.where(:id => self.id)
    end
    users
  end

  def transferable (org = nil)
    return TransferUserResources.transferable(self, org)
  end

  def transfer (org, delegate_owner, delegate_org = nil)
    return TransferUserResources.transfer(self, org, delegate_owner, delegate_org)
  end

  def account_summary (access_role = :all, org = nil)
    summary = Hash.new
    team_ids = self.teams(org, access_role: :all).ids

    models = [DataSource, DataSet, DataSink, DataMap]

    models.each do |model|
      next if !model.respond_to?(:summary)
      summary[model.name.underscore.pluralize.to_sym] = model.summary(self, org, team_ids.length > 0 ? team_ids : nil)
    end
    summary
  end

  def update_user_notification_settings_org(org)
    notification_channel_settings = NotificationChannelSetting.where(:owner_id => self.id)
    notification_channel_settings.each do |setting|
      setting.org_id = org.id
      setting.save!
    end

    notification_setting = NotificationSetting.where(:owner_id => self.id)
    notification_setting.each do |setting|
      setting.org_id = org.id
      setting.save!
    end
  end

  def update_user_default_notification_settings
    notification_types = NotificationType.where(:default => 1)
    channels = ['APP', 'EMAIL']
    org_id = nil
    if !self.default_org.nil?
      org_id = self.default_org.id
    end

    notification_types.each do |notification_type|
      channels.each do |channel|
        notification_channel_setting = NotificationChannelSetting.where(
          :channel => channel, :owner_id => self.id, :org_id => org_id).first

        if notification_channel_setting.nil?
          notification_channel_setting = NotificationChannelSetting.new
          notification_channel_setting.set_defaults(self, self.default_org)
          notification_channel_setting.channel = channel
          notification_channel_setting.save
        end

        notification_setting = NotificationSetting.new
        notification_setting.set_defaults(self, self.default_org)
        notification_setting.notification_type_id = notification_type.id
        notification_setting.resource_id = self.id
        notification_setting.channel = channel
        notification_setting.status = 'ACTIVE'
        notification_setting.notification_channel_setting_id = notification_channel_setting.id
        notification_setting.save
      end
    end
  end

  def self.validate_password(email, full_name, password)
    entropy = get_password_checker(email, full_name).calculate_entropy(password)
    tmp_user = User.new(password: password)
    tmp_user.validate

    {
      entropy: entropy,
      min_entropy: MIN_PASSWORD_ENTROPY,
      errors: tmp_user.errors[:password]
    }
  end

  def rate_limit_parent
    org
  end

  def domain_custodian?(domain_id = :any)
    return DomainCustodian.where(user: self).exists? if domain_id == :any

    DomainCustodian.where(user: self, domain_id: domain_id).exists?
  end

  def org_custodian?(org_id)
    OrgCustodian.where(user: self, org_id: org_id).exists?
  end

  def self.get_password_checker(email, full_name)
    extra_words = (Extra_Words + [email, full_name]).compact
    StrongPassword::StrengthChecker.new(
      min_entropy: MIN_PASSWORD_ENTROPY,
      use_dictionary: true,
      extra_dictionary_words: extra_words
    )
  end

  protected

  def password_weak?
    # We use a custom validator here instead of StrongPassword's
    # built-in validator because we DO NOT want to fire the
    # password-strength validator unless we are updating
    # password attributes. Otherwise we would generate
    # too many errors when updating non-password attributes on
    # existing user records with previously-ok passwords that
    # would not pass the new password-strength test.
    if self.password_changed?
      password_checker = User.get_password_checker(self.email, self.full_name)
      if password_checker.is_weak?(self.password)
        self.errors.add(:password, "is too weak")
      end
    end
  end

  def handle_after_create
    update_user_default_notification_settings
    generate_password_reset_token(true)
    NotificationService.new.publish_new_user(self)

    unless suppress_event_notifications
      # 1 second delay so DB transaction is committed
      UserEventsWebhooksWorker.perform_in(1.second, 'user_created', { user_id: self.id, org_id: self.default_org_id, acquisition_type: 'regular' }.as_json )
    end
  end
end
