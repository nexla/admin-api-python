class DataCredentialsGroup < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org, required: true

  has_many :data_sources
  has_many :data_sinks
  has_many :data_credentials_memberships, dependent: :destroy
  has_many :data_credentials, through: :data_credentials_memberships

  def self.build_from_input(api_user_info, input)
    return nil if api_user_info.blank?
    return nil unless input.is_a?(Hash)

    input.symbolize_keys!

    group = DataCredentialsGroup.new
    group.set_defaults(api_user_info)
    group.update_mutable!(api_user_info, input)

    return group
  end

  def set_defaults(api_user_info)
    self.owner = api_user_info.input_owner
    self.org = api_user_info.input_org
    self.credentials_type = Connector.default_connection_type.type
  end

  def update_mutable!(api_user_info, input)
    return nil if api_user_info.blank?
    return nil unless input.is_a?(Hash)

    input.symbolize_keys!

    self.name = input[:name] if input.key?(:name)
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    if input.key?(:credentials_type)
      if DataCredentials.validate_connector_type(input[:credentials_type]).nil?
        raise Api::V1::ApiError.new(:bad_request, "Invalid connector type")
      end
      self.credentials_type = input[:credentials_type]
    end

    if input.key?(:data_credentials)
      if input[:data_credentials].present?
        credentials = DataCredentials.accessible_by_user(self.owner, self.org, { access_role: :all })
          .where(id: input[:data_credentials], connector_type: self.credentials_type)
        raise Api::V1::ApiError.new(:not_found, "Data credentials not found") if credentials.blank?
      end

      self.data_credentials.replace(credentials)
    end

    self.save!
  end

  def destroy
    data_sources_ids = self.data_sources.ids
    if data_sources_ids.present?
      raise Api::V1::ApiError.new(:method_not_allowed, { data_sources_ids: data_sources_ids, message: "Group cannot be deleted while associated to a source" })
    end

    data_sinks_ids = self.data_sinks.ids
    if data_sinks_ids.present?
      raise Api::V1::ApiError.new(:method_not_allowed, { data_sinks_ids: data_sinks_ids, message: "Group cannot be deleted while associated to a sink" })
    end

    super
  end

  def remove_credentials(api_user_info, credentials_ids)
    return if api_user_info.blank?
    return unless credentials_ids.is_a?(Array)

    credentials = DataCredentials.accessible_by_user(api_user_info.user, api_user_info.org, { access_role: :all })
      .where(id: credentials_ids)
    self.data_credentials.delete(credentials) if credentials.present?
  end

  def flow_attributes(user, org)
    [
      :credentials_type,
      :data_credentials_count
    ].map do |attr|
      case attr
      when :data_credentials_count
        [ attr, self.data_credentials&.count || 0 ]
      else
        [ attr, self.send(attr) ]
      end
    end
  end
end
