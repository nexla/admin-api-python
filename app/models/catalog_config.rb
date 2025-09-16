class CatalogConfig < ApplicationRecord
  include Api::V1::Schema
  include JsonAccessor
  include AuditLog
  include Docs
  include DataplaneConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :data_credentials
  has_many :data_sets_catalog_configs, class_name: "DataSetsCatalogRef", dependent: :destroy
  has_many :data_sets, through: :data_sets_catalog_configs

  enum mode: { auto: 'auto', manual: 'manual'}, _prefix: 'mode'

  json_accessor :config, :templates

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.nil? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end

    config_org = api_user_info.input_org
    
    if (config_org.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input org missing")
    end

    if (!config_org.has_admin_access?(api_user_info.user))
      raise Api::V1::ApiError.new(:forbidden, "Catalog configs can only be created by org admins")
    end

    if (input[:name].blank?)
      raise Api::V1::ApiError.new(:bad_request, "Catalog config name must not be blank")
    end

    input[:mode] = "auto" if (input[:mode].blank?)

    cc = CatalogConfig.new
    cc.update_mutable!(api_user_info, input)
    return cc
  end

  def update_mutable! (api_user_info, input)
    if (input.blank? || api_user_info.nil? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end

    self.update_owner(api_user_info, input)
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)
    self.job_id = input[:job_id] if input.key?(:job_id)
    self.mode = input[:mode] if input.key?(:mode)

    if input.key?(:schedule_time)
      raise Api::V1::ApiError.new( :bad_request, "Invalid schedule time") unless input[:schedule_time].between?(0, 23)
      self.schedule_time = input[:schedule_time]
    end

    # Only for the case when status is being updated.
    if input.key?(:status)
      if input[:status] == "ACTIVE" and CatalogConfig.exists?(org_id: api_user_info.org.id, status: "ACTIVE") 
        raise Api::V1::ApiError.new( :method_not_allowed, "There is already an active catalog configuration for this org (id = #{api_user_info.org.id}). It must be deactivated first.")
      end
      self.status =  input[:status]
    end

    if (input.key?(:data_credentials_id))
      # There should be one Active catalog config at a time.
      # This is to prevent multiple cataloging jobs from running at the same time.
      # If there is an active catalog config, it must be deactivated first.
      # This is requested in PRD https://coda.io/d/Nexla-Product-Hub_dJVJEM4XdYW/PRD-Data-Catalog-Integration_suwtb#_luKJA to simplify the implementation.
      dc = DataCredentials.find_by_id(input[:data_credentials_id])
      if (!dc.nil?)
        if CatalogConfig.exists?(org_id: api_user_info.org.id, data_credentials_id: input[:data_credentials_id]) 
          raise Api::V1::ApiError.new( :method_not_allowed, "There is already a catalog configuration associated to credentails (id = #{input[:data_credentials_id]}). Only one catalog configuration per data credentials (connector_type = #{dc.connector_type}) is allowed.")
        end
        if (dc.org != api_user_info.input_org)
          raise Api::V1::ApiError.new(:bad_request, "Invalid data credentials for catalog config")
        end
        self.data_credentials = dc
      else
        raise Api::V1::ApiError.new(:bad_request, "Data credentials (id = #{input[:data_credentials_id]}) does not exist")
      end
    end
    # config should contain the schema_id
    if input.key?(:config)
      if !input[:config].key?("schema_id") or input[:config]["schema_id"].blank?
        raise Api::V1::ApiError.new(:bad_request, "Schema id is required")
      end
      self.config = input[:config]
    end
    self.save!
  end

  def update_owner (api_user_info, input)
    config_owner = api_user_info.input_owner
    raise Api::V1::ApiError.new(:not_found, "User not found") if config_owner.nil?

    config_org = api_user_info.input_org
    raise Api::V1::ApiError.new(:bad_request, "Input org_id required") if config_org.nil?

    return if (config_owner == self.owner) && (config_org == self.org)

    if (!config_owner.org_member?(config_org))
      raise Api::V1::ApiError.new(:bad_request, "Catalog config owner must be an org member")
    end

    if (!config_org.has_admin_access?(config_owner))
      raise Api::V1::ApiError.new(:bad_request, "Catalog config owner must be an org admin")
    end

    self.owner = config_owner
    self.org = config_org
  end
end
