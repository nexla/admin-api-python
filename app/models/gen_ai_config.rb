class GenAiConfig < ApplicationRecord
  self.inheritance_column = nil

  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  enum status: { active: 'ACTIVE', paused: "PAUSED" }
  enum type: { genai_openai: 'genai_openai', genai_googleai: 'genai_googleai'  }

  json_accessor :config

  belongs_to :owner, class_name: "User"
  belongs_to :org
  belongs_to :data_credentials

  attr_accessor :gen_ai_config_source

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.nil? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end

    config_org = api_user_info.input_org

    raise Api::V1::ApiError.new(:bad_request, "Required input org missing") if config_org.nil?

    unless config_org.has_admin_access?(api_user_info.user)
      raise Api::V1::ApiError.new(:forbidden, "Catalog configs can only be created by org admins")
    end

    raise Api::V1::ApiError.new(:bad_request, "Catalog config name must not be blank") if (input[:name].blank?)

    config = GenAiConfig.new
    config.update_mutable!(api_user_info, input)
    config
  end

  def update_mutable! (api_user_info, input)
    self.transaction do
      if (input.blank? || api_user_info.nil? || api_user_info.input_owner.nil?)
        raise Api::V1::ApiError.new(:bad_request, "Required input missing")
      end

      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

      if input.key?(:name)
        raise Api::V1::ApiError.new(:bad_request, "GenAI config name must not be blank") if input[:name].blank?
        self.name = input[:name]
      end

      if input.key?(:status)
        raise Api::V1::ApiError.new(:bad_request, "Incorrect status") if input.key?(:status) && !GenAiConfig.statuses.keys.include?(input[:status].downcase)
        self.status = input[:status]
      end

      self.description = input[:description] if input.key?(:description)

      if input.key?(:data_credentials_id)
        dc = DataCredentials.find_by(id: input[:data_credentials_id])
        raise Api::V1::ApiError.new(:not_found, "Data credentials (id = #{input[:data_credentials_id]}) do not exist") unless dc

        self.data_credentials = dc
      end

      if (input.key?(:data_credentials_id) || self.type.nil?)
        if  self.data_credentials.connector_type
          self.type = self.data_credentials.connector_type
        end
      end

      raise Api::V1::ApiError.new(:bad_request, "GenAI config type must not be blank") if self.type.blank?

      if self.active? && GenAiConfig.where(org: self.org, status: :active).where.not(id: self.id).exists?
        raise Api::V1::ApiError.new(:bad_request, "An active GenAI config already exists for this org")
      end

      self.config = input[:config] if input.key?(:config)
      self.save!

      if input[:status]
        # Backward compatibility for GenAiOrgSetting we manipulate them when GenAI config status is changed.
        # This will simulate the same behavior as before for UI.
        # UI currently sets GenAI docs ONLY for GenDocs usage.
        if self.active?
          GenAiOrgSetting.find_or_create_by(org: self.org, gen_ai_usage: :gen_docs, gen_ai_config_id: self.id)
          GenAiOrgSetting.where(org: self.org, gen_ai_usage: :gen_docs).where.not(gen_ai_config_id: self.id).destroy_all
        else
          binding = GenAiOrgSetting.find_by(org: self.org, gen_ai_usage: :gen_docs, gen_ai_config_id: self.id)
          binding&.destroy!
        end
      end
    end
  end

end
