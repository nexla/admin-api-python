# frozen_string_literal: true

class ApiAuthConfigValidator < ActiveModel::Validator
  def validate (auth_config)
    # The combination of [org id, service entity id, idp entity id]
    # must be unique, unless the entity ids haven't been set yet.
    return if auth_config.org.nil? ||
      auth_config.service_entity_id.blank? ||
      auth_config.idp_entity_id.blank?

    existing_count = ApiAuthConfig.where(
      :org_id => auth_config.org.id,
      :service_entity_id => auth_config.service_entity_id,
      :idp_entity_id => auth_config.idp_entity_id
    ).where.not(:id => auth_config.id).count()

    if (existing_count > 0)
      auth_config.errors.add :base, "service_entity_id/idp_entity_id combination already in use"
    end
  end
end

class ApiAuthConfig < ApplicationRecord

  class SecretConfigMarshaler
    def dump (value)
      value.to_json
    end
    def load (value_json)
      begin
        JSON.parse(value_json)
      rescue
        {}
      end
    end
  end

  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor

  Protocols = {
    :saml => "saml",
    :oidc => "oidc",
    :password => "password",
    :google => "google",
    :omni => "omni"
  }.freeze
  Default_Idp_Protocol = Protocols[:saml]

  Default_Name_Identifier_Format = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"
  Default_Security_Settings = {
    :authn_requests_signed => false,
    :logout_requests_signed => false,
    :logout_responses_signed => false,
    :metadata_signed => false,
    :digest_method => XMLSecurity::Document::SHA1,
    :signature_method => XMLSecurity::Document::RSA_SHA1
  }

  validates_with ApiAuthConfigValidator
  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org

  json_accessor :security_settings, :oidc_id_claims, :oidc_access_claims, :client_config

  attr_encrypted :secret_config,
    prefix: "", suffix: "_enc",
    marshal: true, marshaler: SecretConfigMarshaler.new, dump_method: :dump, load_method: :load,
    key: :enc_key, encode: "m0", encode_iv: "m0",
    encode_salt: true

  def enc_key
    API_SECRETS[:enc][:auth_config_key][0..31]
  end

  before_save  do
    self.uid = ApiAuthConfig.generate_uid if self.uid.blank?
    self.security_settings = Default_Security_Settings if self.security_settings.blank?
  end

  def self.generate_uid
    tmp_uid = nil

    1.upto(5) do
      tmp_uid = rand(36**8).to_s(36)
      break if ApiAuthConfig.select(:uid).find_by_uid(tmp_uid).nil?
    end

    if (tmp_uid.nil?)
      raise Api::V1::ApiError.new(:internal_server_error, "Could not generate org idp uid")
    end

    tmp_uid
  end

  def self.generate_base_url (request)
    proto = (Rails.env.development? ? "http" : "https") + 
      "://" + request.env["SERVER_NAME"]

    port = request.env["SERVER_PORT"]
    proto += ":#{port}" if (!port.blank? && (port != "80"))

    context = ""
    if (Rails.env.qa?)
      context = "admin-api"
    elsif (!Rails.env.development?)
      context = "nexla-api"
    end
    
    proto += "/#{context}" if !context.blank?
    return proto
  end

  def self.sso_options
    # BEWARE this method is called from an unauthenticated route.
    # Be careful about what attributes are revealed.
    ApiAuthConfig.where(global: true).map(&:public_attributes) || []
  end

  def self.get_mapping_config(uid)
    mapping = ApiAuthConfig.find_by(uid: uid)
    return mapping if mapping.nil?
    return mapping.secret_config.is_a?(Hash) ?
      mapping.client_config.deep_merge(mapping.secret_config) : mapping.client_config
  end

  def self.build_from_input (api_user_info, input, request)
    if (input.blank? || api_user_info.nil? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end

    mapping_org = api_user_info.input_org
    if (mapping_org.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input org missing")
    end

    if (!mapping_org.has_admin_access?(api_user_info.user))
      raise Api::V1::ApiError.new(:forbidden, "IDP mappings can only be created by org admins")
    end

    if (input.key?(:global) && input[:global] && !api_user_info.user.super_user?)
      raise Api::V1::ApiError.new(:forbidden, "Global mappings can only be created by Nexla administrators.")
    end

    if (input.key?(:uid) && !input.key?(:global))
      raise Api::V1::ApiError.new(:bad_request, "UID can only be specified for global mappings")
    end

    protocol = (input[:protocol] || Default_Idp_Protocol).downcase
    if (Protocols.key?(protocol))
      raise Api::V1::ApiError.new(:bad_request, "Unsupported IDP protocol")
    end

    if (input[:name].blank?)
      raise Api::V1::ApiError.new(:bad_request, "Mapping name must not be blank")
    end

    auth_config = ApiAuthConfig.new
    auth_config.uid = ApiAuthConfig.generate_uid
    auth_config.protocol = protocol
    auth_config.update_mutable!(api_user_info, input, request)

    return auth_config
  end

  def update_mutable! (api_user_info, input, request)
    self.update_mapping_owner(api_user_info, input)
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)

    if (input.key?(:global) && input[:global] && !api_user_info.user.super_user?)
      raise Api::V1::ApiError.new(:forbidden, "Global mappings can only be created by Nexla administrators.")
    end
    self.global = input[:global] if input.key?(:global)

    if input.key?(:uid)
      if !input.key?(:global)
        # we check this here as well as in build_from_input to check for both update and
        # create calls while still being able to error out before creating a new mapping
        #
        # Custom UIDs are only allowed for global mappings, since they can be used to define the path for authenticating
        # via a specif auth method.
        #
        # For example, if a global mapping with UID "myidp" is created, then the path for authenticating via that IDP
        # would be /token/myidp
        raise Api::V1::ApiError.new(:bad_request, "UID can only be specified for global mappings")
      elsif ApiAuthConfig.find_by_uid(input[:uid]).present?
        raise Api::V1::ApiError.new(:bad_request, "UID already in use")
      else
        self.uid = input[:uid]
      end
    end

    self.auto_create_users_enabled = input[:auto_create_users_enabled] if input.key?(:auto_create_users_enabled)

    if (!input[:nexla_base_url].blank? || self.nexla_base_url.blank?)
      base_url = input[:nexla_base_url].blank? ?
        ApiAuthConfig.generate_base_url(request) : input[:nexla_base_url]
      while ("/" == base_url.last) do
        base_url.chomp!("/")
      end
      self.nexla_base_url = base_url if !base_url.blank?
    end

    if self.is_saml?
      self.name_identifier_format = (input[:name_identifier_format] || 
        Default_Name_Identifier_Format)

      if (!input[:service_entity_id].blank? || self.service_entity_id.blank?)
        self.service_entity_id = input[:service_entity_id].blank? ?
          self.nexla_base_url : input[:service_entity_id]
      end

      self.assertion_consumer_url = input[:assertion_consumer_url] if input.key?(:assertion_consumer_url)
      self.idp_entity_id = input[:idp_entity_id] if input.key?(:idp_entity_id)
      self.idp_sso_target_url = input[:idp_sso_target_url] if input.key?(:idp_sso_target_url)
      self.idp_slo_target_url = input[:idp_slo_target_url] if input.key?(:idp_slo_target_url)
      self.idp_cert = input[:idp_cert] if input.key?(:idp_cert)
    elsif self.is_oidc?
      self.oidc_domain = input[:oidc_domain] if input.key?(:oidc_domain)
      self.oidc_keys_url_key = input[:oidc_keys_url_key] if input.key?(:oidc_keys_url_key)
      self.oidc_id_claims = input[:oidc_id_claims] if input[:oidc_id_claims].is_a?(Hash)
      self.oidc_access_claims = input[:oidc_access_claims] if input[:oidc_access_claims].is_a?(Hash)
    end

    self.security_settings = input[:security_settings] if input.key?(:security_settings)
    self.metadata = input[:metadata] if input.key?(:metadata)
    self.client_config = input[:client_config] if input.key?(:client_config)
    self.secret_config = input[:secret_config] || {}
    self.save!
  end

  Protocols.keys.each do |protocol_name|
    define_method "is_#{protocol_name}?" do
      self.protocol == Protocols[protocol_name]
    end
  end

  def public_attributes

    # BEWARE this method is called from an unauthenticated route.
    # Be careful about what attributes are revealed.
    load_public_attribute_config

    attrs = {}

    if @@auth_config_spec.present?
      keys = @@auth_config_spec["COMMON"] + (@@auth_config_spec[self.protocol] || [])
      keys.each do |key|
        if self.respond_to?(key)
          attrs[key] = self.public_send(key)
          if (key == "client_config")
            attrs.dig(key, "web")&.delete("client_secret")
          end
        end
      end
    end

    return attrs
  end

  def load_public_attribute_config
    @@auth_config_spec ||= nil
    return if !@@auth_config_spec.nil?
    config_spec = JSON.parse(File.read("#{Rails.root}/config/AuthConfigPublicAttributes.json"))
    @@auth_config_spec = config_spec if config_spec["COMMON"].present?
  end

  def assertion_consumer_url
    return nil if !self.is_saml?
    acu = super
    return (acu.blank? ? "#{self.nexla_base_url}/token/#{self.uid}" : acu)
  end

  def logout_url
    "#{self.nexla_base_url}/logout/#{self.uid}"
  end

  def metadata_url
    "#{self.nexla_base_url}/metadata/#{self.uid}"
  end

  def oidc_token_verify_url
    self.is_oidc? ? "#{self.nexla_base_url}/token/#{self.uid}" : nil
  end

  def get_assertion_settings
    settings = OneLogin::RubySaml::Settings.new

    settings.soft = true
    settings.issuer = self.service_entity_id
    settings.assertion_consumer_service_url = self.assertion_consumer_url
    settings.assertion_consumer_logout_service_url = self.logout_url

    settings.idp_entity_id = self.idp_entity_id
    settings.idp_sso_target_url = self.idp_sso_target_url
    settings.idp_slo_target_url = self.idp_slo_target_url
    settings.idp_cert = self.idp_cert
    settings.name_identifier_format = self.name_identifier_format

    ss = self.security_settings
    ss = Default_Security_Settings if ss.blank?

    ss.keys.each do |key|
      settings.security[key] = ss[key]
    end

    return settings
  end

  protected

  def update_mapping_owner (api_user_info, input)
    mapping_owner = api_user_info.input_owner
    raise Api::V1::ApiError.new(:not_found, "User not found") if mapping_owner.nil?

    mapping_org = api_user_info.input_org
    raise Api::V1::ApiError.new(:bad_request, "Input org_id required") if mapping_org.nil?

    return if (mapping_owner == self.owner) && (mapping_org == self.org)

    if (!mapping_owner.org_member?(mapping_org))
      raise Api::V1::ApiError.new(:bad_request, "Mapping owner must be an org member")
    end

    if (!mapping_org.has_admin_access?(mapping_owner))
      raise Api::V1::ApiError.new(:bad_request, "Mapping owner must be an org admin")
    end

    self.owner = mapping_owner
    self.org = mapping_org
  end

end
