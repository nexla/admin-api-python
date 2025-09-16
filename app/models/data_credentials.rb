class DataCredentialsValidator < ActiveModel::Validator
  def validate (dc)
    # If the DataCredentials is created within an org, the owner must be
    # a member of that org
    if (!dc.org.nil? && !dc.owner.org_member?(dc.org))
      dc.errors.add :base, "Owner not in DataCredentials organization"
    end

    dc.errors.add :base, "Invalid credentials type" if !DataCredentials.validate_connector_type(dc.connector_type)
    dc.errors.add :base, "Connector is required" if dc.connector.nil?
  end
end

class DataCredentials < ApplicationRecord
  self.primary_key = :id
  self.table_name = "data_credentials"

  using Refinements::HttpResponseString

  include Api::V1::Schema
  include Api::V1::Auth
  include AccessControls::Standard
  include Accessible
  include FlowNodeData
  include AuditLog
  include Copy
  include SearchableConcern
  include DataplaneConcern
  include ReferencedResourcesConcern
  include ChangeTrackerConcern

  SKIP_ATTRIBUTES_IN_SEARCH = %w[users_api_key_id credentials_enc credentials_enc_iv].freeze

  class CredentialsMarshaler
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

  validates_with DataCredentialsValidator

  attr_encrypted :credentials,
    prefix: "", suffix: "_enc",
    marshal: true, marshaler: CredentialsMarshaler.new, dump_method: :dump, load_method: :load,
    key: :enc_key, encode: "m0", encode_iv: "m0",
    encode_salt: true

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :vendor
  belongs_to :auth_template, class_name: "AuthTemplate", foreign_key: "auth_template_id", required: false
  belongs_to :copied_from, class_name: "DataCredentials", foreign_key: "copied_from_id"
  belongs_to :users_api_key, dependent: :destroy
  alias_method :api_key, :users_api_key
  belongs_to :connector, foreign_key: "connector_type", primary_key: "type"

  has_many :quarantine_settings, dependent: :restrict_with_error
  has_many :data_credentials_memberships, dependent: :destroy
  has_many :data_credentials_groups, through: :data_credentials_memberships, class_name: 'DataCredentialsGroup'

  attr_writer :credentials_non_secure_data, :template_config

  acts_as_taggable_on :tags
  def tags_list
    self.tags.map(&:name)
  end
  alias_method :tag_list, :tags_list

  mark_as_referenced_resource
  referencing_resources :data_maps

  before_save :handle_before_save
  after_commit :handle_after_commit, on: [:create, :update]
  before_destroy :handle_before_destroy

  scope :by_credentials_type, ->(credentials_type) {
    by_vendor_name(credentials_type).or( with_vendors.where(connector_type: credentials_type) ).reselect("data_credentials.*")
  }

  scope :by_vendor_name, ->(vendor_name) {
    with_vendors.where(vendors: { name: vendor_name })
  }
  scope :with_vendors, ->{  joins('left outer join vendors on data_credentials.vendor_id=vendors.id' ) }

  Verified_Status_Max_Length = 1024

  def self.validate_connector_type (connector_type)
    return DataSink.connector_types.key(connector_type.to_s)
  end

  def api_keys
    self.api_key.present? ? [ self.api_key ] : []
  end

  def credentials_non_secure_data
    load_config
    credentials = self.credentials
    credentials_data = {}
    if !(@@config_spec.nil?) and !credentials.nil?
      config_credentials_type = @@config_spec["spec"][self.connector_type]

      if !(config_credentials_type.nil?) and !((config_credentials_type["non_secure_config"]).nil?)
        non_secure_config = config_credentials_type["non_secure_config"]
        credentials.each do |key, val|
          if non_secure_config.include? key
            credentials_data[key] = val
          end
        end
      end
    end
    return credentials_data
  end

  def template_config(secure_fields = false)
    template_config = {}
    if !self.vendor.nil?
      if secure_fields
        template_config = self.credentials
      else
        credentials = self.credentials
        auth_parameters = AuthParameter.where(:vendor_id => self.vendor_id)
        if !secure_fields
          auth_parameters = auth_parameters.where(:secured => secure_fields)
        end
        auth_params = auth_parameters.map(&:name)
        auth_params.each do |auth_param|
          cred_value = credentials[auth_param]
          template_config[auth_param] = cred_value if !cred_value.nil?
        end
      end
    end
    return template_config
  end

  def enc_key
    API_SECRETS[:enc][:credentials_key][0..31]
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

  def credentials_type
    self.connector_type
  end

  def update_mutable! (api_user_info, input, request)
    return if (input.blank? || api_user_info.nil?)
    input.symbolize_keys!

    ref_fields = input.delete(:referenced_resource_ids)
    verify_ref_resources!(api_user_info.input_owner, ref_fields)

    tags = input.delete(:tags)

    self.credentials_version = input[:credentials_version] if input.key?(:credentials_version)
    self.name = input[:name] if input.key?(:name)
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    if input.key?(:verified_status)
      self.verified_status = input[:verified_status]
    end

    if input.key?(:credentials_type)
      # Convert backwards-compatible key to new key
      input[:connector_type] = input[:credentials_type] if !input.key?(:connector_type)
      input.delete(:credentials_type)
    end

    if input.key?(:connector_type)
      if DataCredentials.validate_connector_type(input[:connector_type]).nil?
        raise Api::V1::ApiError.new(:bad_request, "Invalid connector type")
      end
      self.connector_type = input[:connector_type]
    end

    if (input.key?(:credentials) && (input[:credentials].class == Hash))
      v = CodeUtils.validate_config(input[:credentials])
      raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?

      input[:credentials].symbolize_keys!

      # Note, we set :credentials_type in the :credentials object for
      # backwards-compatibility with infrastructure, which depends on
      # it being set there. That dependency could/should be removed
      # in the infrastructure code. Also note: we use the underlying
      # connection_type, which is what the infrastructure will use to
      # probe/ingest.
      input[:credentials][:credentials_type] = self.connector.present? ?
        self.connector.connection_type : self.connector_type
      self.credentials = input[:credentials]
    elsif (input[:connector_type] == DataSource.connector_types[:nexla_monitor])
      generate_api_key api_user_info
      self.credentials = generate_nexla_monitor_rest_credentials(request)
    elsif ((input.key?(:vendor_id) || input.key?(:vendor_name)) && input.key?(:template_config))
      vendor = input.key?(:vendor_name) ? Vendor.find_by_name(input[:vendor_name]) :
        Vendor.find(input[:vendor_id])
      raise Api::V1::ApiError.new(:bad_request, "Invalid vendor") if vendor.nil?

      if vendor.auth_templates.count > 1
        if input.key?(:auth_template_id)
          auth_template = AuthTemplate.where(vendor_id: vendor.id, id: input[:auth_template_id]).first
        elsif input.key?(:auth_template_name)
          auth_template = AuthTemplate.where(vendor_id: vendor.id, name: input[:auth_template_name]).first
        else
          # throw error if multiple templates are available and no template is selected
          raise Api::V1::ApiError.new(:bad_request, "Multiple templates found. Select an auth template to use.") if auth_template.nil?
        end
      else
        auth_template = vendor.auth_templates.first
      end
      raise Api::V1::ApiError.new(:bad_request, "No auth template found") if auth_template.blank?

      self.auth_template = auth_template

      auth_params = AuthParameter.where("auth_template_id = :auth_template_id or global = :global",
        { auth_template_id: auth_template.id, global: true }).map(&:name)

      add_data_cred_params(auth_params, input[:template_config])
      template_config = input[:template_config]
      credentials = {}

      auth_template.config.each do |key, value|
        if value.to_s.include?("${")
          value_str = value.to_s

          should_convert_to_int = value_str.starts_with?("${") && value_str.ends_with?("}.as_integer") ? true : false
          template_val = value_str
          template_val.chomp!(".as_integer") if should_convert_to_int

          auth_params.each do |parameter|
            template_key = "${#{parameter}}"
            template_val = template_val.gsub(template_key,template_config[parameter].to_s) if !template_config[parameter].nil?
          end

          # TODO: probably need a better method of ensuring the final resource config can have non-string datatypes.
          # if value is of format "${...}.as_integer" then convert to integer.
          credentials[key] = should_convert_to_int ? template_val.to_i : template_val
        else
          credentials[key] = value
        end
      end

      credentials.symbolize_keys!
      self.connector_type = auth_template.connector.type
      credentials[:credentials_type] = self.connector_type
      self.vendor_id = vendor.id
      VendorEndpoint.validate_config(credentials, auth_params)
      self.credentials = credentials
    end

    self.save!

    ResourceTagging.add_owned_tags(self, { tags: tags }, api_user_info.input_owner)

    self.update_referenced_resources(ref_fields)
  end

  def generate_api_key api_user_info
    self.users_api_key = api_user_info.user
      .build_api_key_from_input(api_user_info,
        {
          name: "Static API Key",
          description: "Auto-generated for Nexla Monitor credentials",
          scope: UsersApiKey::Scopes[:nexla_monitor]
        }
      )
  end

  def verified_status= (status_str)
    status_str = status_str.to_s[0...Verified_Status_Max_Length]

    if status_str.split(' ')[0].to_s.success_code?
      self.verified_at = Time.now
      # NEX-5955 UI depends on this specific status string. We
      # can remove this assignment when UI switches to checking
      # verified_at.
      status_str = "200 Ok"
    else
      self.verified_at = nil
    end

    write_attribute(:verified_status, status_str.presence)
  end

  def add_data_cred_params(auth_params, config)
    if config.is_a?(Hash)
      config.each do |key, value|
        auth_params.push(key)
      end
    end
  end

  def validate_gdrive_credentials (request, user)
    if !self.credentials[:one_time_code].blank? && self.credentials[:access_token].blank?
      result = exchange_google_one_time_code(user.email, self.credentials, request)
      raise Api::V1::ApiError.new(:bad_request, "Invalid Google Drive credentials") if result.nil?
      result = JSON.parse(result.to_json).merge(self.credentials)
      self.credentials = result
    end
    if self.credentials["access_token"].blank? && self.credentials[:access_token].blank?
      raise Api::V1::ApiError.new(:bad_request, "Invalid Google Drive credentials type")
    end
  end

  def refresh
    return if self.credentials_type != DataSource.connector_types[:gdrive]
    credentials = refresh_google_credentials(self.credentials)
    raise Api::V1::ApiError.new(:bad_request, credentials) if credentials.is_a?(String)
    self.credentials = credentials
    self.save!
  end

  def has_credentials?
    DataCredentials.validate_connector_type(self.connector_type).present? && !self.credentials.blank?
  end

  def resources
    result = {}

    result[:data_sources] = DataSource.where(data_credentials: self).ids
    result[:data_sinks] = DataSink.where(data_credentials: self).ids
    result[:data_sets] = DataSet.where(data_credentials: self).ids
    result[:gen_ai_configs] = GenAiConfig.where(data_credentials: self).ids
    result[:catalog_configs] = CatalogConfig.where(data_credentials: self).ids
    result[:code_containers] = (CodeContainer.where(data_credentials: self).ids + CodeContainer.where(runtime_data_credentials: self).ids).uniq

    return nil if result.values.all?(&:blank?)

    result
  end

  def origin_node_ids
    ids = []
    [DataSource, DataSink].each do |m|
      ids << m.where(data_credentials_id: self.id).pluck(:origin_node_id)
    end
    ids = ids.flatten.uniq.compact
    ids
  end

  def origin_nodes
    FlowNode.where(id: self.origin_node_ids)
  end

  def flow_origins
    ## REMOVE once flow_nodes transition is done
    ##
    data_sources = DataSource.where(data_credentials: self).ids
    fo = { :data_sources => data_sources, :data_sets => Array.new }

    dfo = DataSink.where(data_credentials: self).map(&:flow_origin)
    fo[:data_sources] += dfo.select { |f| f.is_a?(DataSource) }.map(&:id)
    fo[:data_sets] += dfo.select { |f| f.is_a?(DataSet) }.map(&:id)

    return fo
  end

  def load_config
    @@config_spec ||= nil
    return if !@@config_spec.nil?
    config_spec = JSON.parse(File.read("#{Rails.root}/config/ConnectorCredentialSpec.json"))
    @@config_spec = config_spec if !((config_spec["spec"]).nil?)
  end

  def has_template?
    (!(self.vendor_id.nil?) && !self.vendor.nil?)
  end

  def vendor_id
    self.vendor&.id
  end

  def vendor_info
    v = nil
    if self.vendor.present?
      v = {
        :id => self.vendor.id,
        :name => self.vendor.name
      }
    end
    return v
  end

  def update_api_key api_key
    if (self.credentials["request.headers"].present?)
      creds = self.credentials
      creds["request.headers"] = "Authorization:Basic #{api_key}"
      self.credentials = creds
      self.save!
    end
  end

  def generate_nexla_monitor_rest_credentials (request)
    base_url = ApiAuthConfig.generate_base_url(request)
    return {
      # Note, we replace the infrastructure-facing credentials type
      # with the underlying connection type, which is what
      # the infrastructure expects. This could be handled on the backend.
      "credentials_type" => self.connector.connection_type,
      "auth.type" => "NONE",
      "ignore.ssl.cert.validation" => false,
      "test.method" => "GET",
      "test.content.type" => "application/json",
      "jwt.enabled" => false,
      "hmac.enabled" => false,
      "request.headers" => "Authorization:Basic #{self.api_key.api_key}",
      "test.url" => "#{base_url}/notifications?page=1&per_page=5"
    }
  end

  def as_json (options = nil)
    # Override the default as_json method to allow
    # decrypting the 'credentials' attribute if requested
    j = super()
    j["credentials"] = self.credentials.as_json if (options.is_a?(Hash) && options[:decrypt])
    return j
  end

  def encrypted_credentials
    {
      credsEnc: self.credentials_enc,
      credsEncIv: self.credentials_enc_iv,
      credsId: self.id
    }
  end

  def versions_ignore_update
    (self.changes.keys == ["verified_at"])
  end

  def raw_credentials_type (user)
    if user.is_a?(User) && user.infrastructure_user?
      return self.connector.connection_type
    end

    self.connector_type
  end

  def searchable_attributes
    attrs = self.attributes.except( *DataCredentials::SKIP_ATTRIBUTES_IN_SEARCH )
    attrs[:credentials_type] = self.credentials_type

    attrs[:connector_code] = search_connector_code
    attrs[:connector_name] = search_connector_name
    attrs
  end

  def search_connector_code
    vendor_code = vendor&.name
    connector_type = self.connector_type
    wrap_vendor_parts([vendor_code, connector_type, vendor&.connection_type])
  end

  def search_connector_name
    vendor_name = vendor&.display_name
    connector_name = self.connector&.name
    wrap_vendor_parts([vendor_name, connector_name, vendor&.connection_type])
  end

  def flow_attributes (user, org)
    [
      :credentials_type,
      :verified_status,
      :managed,
      :template_config,
      :vendor
    ].map do |attr|
      case attr
      when :vendor
        [ :vendor, self.vendor_info ]
      else
        [ attr, self.send(attr) ]
      end
    end
  end

  protected

  def handle_before_save
    PaperTrail.request.disable_model(DataCredentials) if self.versions_ignore_update
    return true
  end

  def handle_after_commit
    PaperTrail.request.enable_model(DataCredentials)
    ControlService.new(DataCredentials.find(self.id)).publish(:update)
  end

  def handle_before_destroy
    resources = self.resources
    if resources.present?
      resources[:message] = "Data credentials in use"
      raise Api::V1::ApiError.new(:method_not_allowed, resources)
    end

    ControlService.new(self).publish(:delete)
  end

end
