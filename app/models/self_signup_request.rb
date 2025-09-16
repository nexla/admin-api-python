class SelfSignupRequest < ApplicationRecord
  DAYS_TO_KEEP = 7
  TOKEN_EXPIRE_HOURS = 24
  ADDITIONAL_INFO_FORMAT = /\A(?!.*--)[\w\s'.,&@!-]+\z/

  include AuditLog
  include Api::V1::Schema

  validates_presence_of :email, :full_name, :email_verification_token
  validates_format_of :full_name, with: ADDITIONAL_INFO_FORMAT

  before_validation :generate_email_verification_token

  enum status: { pending: 'PENDING', email_verified: 'EMAIL_VERIFIED', rejected: 'REJECTED', approved: 'APPROVED'}

  attr_accessor :existing_user
  attr_accessor :sso_used

  belongs_to :invite
  has_one :org_additional_info, dependent: :nullify

  def self.verify_invite!(invite)
    raise Api::V1::ApiError.new(:bad_request, "Invalid invite") if invite.nil?

    org = invite.org
    if org.self_signup? && org.members.count >= org.members_limit
      raise Api::V1::ApiError.new(:bad_request, "Can't add more members to the organization")
    end
  end

  def self.build_from_input(input, invite_uid, existing_user, origin)
    additional_info = build_additional_info(input)
    if existing_user
      if existing_user.orgs.where(self_signup: true).exists?
        Rails.logger.info("Self-signup: signup failed for existing user #{existing_user.id} (email #{existing_user.email}) - already has self-signup org")
        raise Api::V1::ApiError.new(:bad_request, "You already have a self-signed organization.")
      end

      signup_request = SelfSignupRequest.new(existing_user: existing_user, sso_used: input[:sso_used])
      signup_request.org_additional_info = additional_info
      org = signup_request.create_user_and_org!
      additional_info.org_id = org.id
      additional_info.save!

      return { org_id: org.id, result: :ok }
    else
      invite = Invite.find_by(uid: invite_uid)
      verify_invite!(invite) if invite_uid

      signup_request = SelfSignupRequest.new(email: invite&.invitee_email || input[:email],
                                             full_name: input[:full_name],
                                             invite_id: invite&.id,
                                             sso_used: input[:sso_used])
      signup_request.validate_email!(existing_user)
      signup_request.save!

      additional_info.self_signup_request_id = signup_request.id
      additional_info.save!

      if signup_request.sso_used
        org = signup_request.create_user_and_org!
        additional_info.org_id = org.id
        additional_info.save!

        return { org_id: org.id, result: :ok }
      else
        NotificationService.new.publish_email_verification(signup_request.email, signup_request.full_name, origin, signup_request.email_verification_token)

        return { result: :ok }
      end
    end
  end

  def self.build_additional_info(input)
    org_additional_info = OrgAdditionalInfo.new(data: input[:personal_info])

    if org_additional_info.invalid?
      message = org_additional_info.errors.full_messages.join(" ")
      raise Api::V1::ApiError.new(:bad_request, message)
    end

    org_additional_info
  end

  def verify!
    if email_verification_token_expires_at.past?
      Rails.logger.info("Self-signup: verify_email rejected - verification token expired for email: #{email}")
      raise Api::V1::ApiError.new(:bad_request, 'Verification token expired')
    end
    update!(status: :email_verified, email_verified_at: Time.now)
  end

  def approve!(approved_by=nil)
    update!(status: :approved, approved_at: Time.now, reviewed_by_user_id: approved_by&.id)
    if approved_by
      Rails.logger.info("Self-signup: request was approved for email '#{email}' by user: #{approved_by.email}")
    else
      Rails.logger.info("Self-signup: request was auto-approved for email: '#{email}'")
    end
  end

  def create_user_and_org!
    cluster_uid = ENV['API_SELF_SIGNUP_DATAPLANE']
    if cluster_uid.nil?
      Rails.logger.error("Self-signup: API_SELF_SIGNUP_DATAPLANE is not set")
      raise Api::V1::ApiError.new(:bad_request, "Can't create an account" )
    end
    cluster = Cluster.find_by(uid: cluster_uid)
    if cluster.nil?
      Rails.logger.error("Self-signup: cluster for self-signup with uid #{cluster_uid} (API_SELF_SIGNUP_DATAPLANE) wasn't found")
      raise Api::V1::ApiError.new(:bad_request, "Can't create an account" )
    end

    if self.existing_user
      user = self.existing_user
      email = user.email
      Rails.logger.info("Self-signup: using existing user #{user.id} (email #{email})")
    else
      email = self.email
      tmp_password = User.create_temporary_password
      user = User.create!(email: email,
                          full_name: full_name,
                          password: tmp_password,
                          password_confirmation: tmp_password,
                          suppress_event_notifications: true)
      Rails.logger.info("Self-signup: created user #{user.id} (email #{email})")
    end

    domain = email.split('@').last
    name = "#{domain}-#{SecureRandom.hex(4)}"

    org = nil
    if self.invite
      org = self.invite.org
      org.add_members(user)
    end

    if org.nil?
      org_tier = OrgTier.find_by(name: 'SELF_SIGNUP_TRIAL')

      org_input = {
        owner_id: user.id,
        cluster_id: cluster.id,
        name: name,
        email_domain: domain,
        org_tier_id: org_tier.id,
        allow_api_key_access: true,
        self_signup: true
      }
      org = Org.build_from_input(org_input, user, nil)
      self.org_additional_info.update!(org_id: org.id)
      Rails.logger.info("Self-signup: created org #{org.id} for user #{user.id} (email #{email})")
    else
      Rails.logger.info("Self-signup: using existing org #{org.id} for user #{user.id} (email #{email})")
    end

    unless self.existing_user
      user.update(default_org_id: org.id)
      payload = {
        user_id: user.id,
        org_id: org.id,
        acquisition_type: self.invite ? 'invitation' : 'self-signup'
      }
      UserEventsWebhooksWorker.perform_async('user_created', payload.as_json)
    end

    self.update!(created_user_id: user.id, created_org_id: org.id) unless existing_user
    org
  end

  def validate_email!(existing_user=nil)
    return true if existing_user

    email = self.email.downcase

    unless email =~ URI::MailTo::EMAIL_REGEXP
      raise Api::V1::ApiError.new(:bad_request, 'Email is invalid')
    end

    domain = email.split('@').last
    if SelfSignupBlockedDomain.where(domain: domain).exists?
      Rails.logger.info("Self-signup: signup failed by blocked domain: #{domain}")
      raise Api::V1::ApiError.new(:bad_request, "Can’t create an account")
    end

    similar_email = SelfSignupRequest.find_user_by_similar_email(email)
    if similar_email
      Rails.logger.info("Self-signup: signup failed due to similar or same email: '#{email}' is similar to '#{similar_email.email}'")
      raise Api::V1::ApiError.new(:bad_request, "Can’t create an account")
    end
  end

  def generate_email_verification_token(force: false)
    return if !force && email_verification_token.present?

    token = nil
    until token && SelfSignupRequest.find_by(email_verification_token: token).nil?
      token = SecureRandom.hex(32)
    end

    self.email_verification_token = token
    self.email_verification_token_expires_at = TOKEN_EXPIRE_HOURS.hours.from_now
    self.email_verified_at = Time.current if self.sso_used
  end

  def self.find_user_by_similar_email(email)
    email = email.downcase
    user = User.find_by(email: email)
    return user if user.present?

    #find existing with dots and pluses
    stripped_email = email.gsub(/\+.*@/, '@').gsub(/\./, '')

    # find uses on same domain
    domain = email.split('@').last
    User.where("email like CONCAT('%', ?)", domain).each do |user|
      # compare without . and +
      stored_email = user.email.downcase.gsub(/\+.*@/, '@').gsub(/\./, '')
      if stored_email == stripped_email
        return user
      end
    end

    nil
  end

  def self.create_sso_org_and_user(api_auth_config, email, full_name)
    unless FeatureToggle.enabled?(:automatic_self_signup_approval)
      raise Api::V1::ApiError.new(:unauthorized, "Creating self-signup orgs is disabled, please contact support.")
    end

    cluster_uid = ENV['API_SELF_SIGNUP_DATAPLANE']
    if cluster_uid.nil?
      Rails.logger.error("Self-signup: API_SELF_SIGNUP_DATAPLANE is not set")
      raise Api::V1::ApiError.new(:bad_request, "Can't create an account" )
    end
    cluster = Cluster.find_by(uid: cluster_uid)
    if cluster.nil?
      Rails.logger.error("Self-signup: cluster for self-signup with uid #{cluster_uid} (API_SELF_SIGNUP_DATAPLANE) wasn't found")
      raise Api::V1::ApiError.new(:bad_request, "Can't create an account" )
    end

    return [nil, nil] if (email.blank? || api_auth_config.nil?)

    org = api_auth_config.org
    return [nil, nil] if org.nil?

    user = User.find_by_email(email)
    user_found = !user.nil?

    if user_found && user.deactivated?
      raise Api::V1::ApiError.new(:unauthorized)
    end

    unless user_found
      unless api_auth_config.auto_create_users_enabled?
        return [nil, nil]
      end

      pwd = User.create_temporary_password
      user = User.create!({
                           email: email,
                           full_name: full_name,
                           default_org: org,
                           password: pwd
                         })
    end

    org_domain = email.split('@').last
    org_name = "#{org_domain}-#{SecureRandom.hex(4)}"
    org_tier = OrgTier.find_by(name: 'SELF_SIGNUP_TRIAL')

    org_input = {
      owner_id: user.id,
      cluster_id: cluster.id,
      name: org_name,
      email_domain: org_domain,
      org_tier_id: org_tier.id,
      allow_api_key_access: true,
      self_signup: true
    }
    org = Org.build_from_input(org_input, user, nil)

    unless user_found
      user.update(default_org_id: org.id)
    end

    return [user, org]
  end
end
