class QuarantineSetting < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor
  include DataplaneConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :data_credentials

  json_accessor :config

  Resource_Types = {
    :org                => 'ORG',
    :user               => 'USER',
    :flow               => 'FLOW',
    :data_flow          => 'DATA_FLOW',
    :custom_data_flow   => 'CUSTOM_DATA_FLOW',
    :data_source        => 'SOURCE',
    :data_set           => 'DATASET',
    :data_sink          => 'SINK'
  }

  def self.resource_types_enum
    enum = "ENUM("
    first = true
    Resource_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.validate_resource_type_str (type_str)
    return nil if type_str.class != String
    return nil if Resource_Types.find { |sym, str| str == type_str }.nil?
    type_str
  end

  def self.build_from_input (api_user_info, input)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    if input.key?(:data_credentials) || input.key?(:data_credentials_id)
      input[:data_credentials_id] = (input[:data_credentials_id] || input[:data_credentials])
      input.delete(:data_credentials)
    else
      raise Api::V1::ApiError.new(:bad_request, "Required data_credentials or data_credentials_id missing from input")
    end

    quarantine_setting = QuarantineSetting.new
    quarantine_setting.set_defaults(api_user_info.input_owner, api_user_info.input_org)

    if input.key?(:resource_type) and input.key?(:resource_id)
      resource_type = QuarantineSetting.validate_resource_type_str(input[:resource_type])
      raise Api::V1::ApiError.new(:bad_request, "Unknown Resource type") if resource_type.nil?

      quarantine_setting.resource_type = input[:resource_type]
      quarantine_setting.resource_id = input[:resource_id]
    else
      quarantine_setting.resource_type = 'USER'
      quarantine_setting.resource_id = api_user_info.input_owner.id
    end

    quarantine_setting.update_mutable!(api_user_info, input)
    return quarantine_setting
  end

  def update_mutable! (api_user_info, input)
    return if input.nil?
    ability = Ability.new(api_user_info.user)

    if input.key?(:data_credentials)
      # Supporting 'data_credential' input attribute
      # for backwards-compatibility
      input[:data_credentials_id] = input[:data_credentials]
      input.delete(:data_credentials)
    end

    self.transaction do 
      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

      if (input.key?(:data_credentials_id))
        data_credentials = DataCredentials.find(input[:data_credentials_id].to_i)
        if (data_credentials.org_id != self.org_id) || !ability.can?(:manage, data_credentials)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
        end
        self.data_credentials = data_credentials
      end

      if (input.key?(:config))
        self.config = input[:config]
      end

      self.save!
    end
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end
end
