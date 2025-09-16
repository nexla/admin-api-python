class GenAiOrgSetting < ApplicationRecord
  include Api::V1::Schema
  include AuditLog

  enum gen_ai_usage: { gen_docs: 'gen_docs', code_check: 'code_check', all: 'all' }, _prefix: 'usage'

  belongs_to :org
  belongs_to :gen_ai_config

  validates_presence_of :gen_ai_config

  def self.build_from_input(input, api_user_info)
    input = input.with_indifferent_access
    if input[:org_id]
      unless api_user_info.user.super_user?
        raise Api::V1::ApiError.new(:forbidden, "Only Nexla administrators can activate configs for other orgs")
      end
      org = Org.find(input[:org_id])
    else
      org = api_user_info.org
      ability = Ability.new(api_user_info.user)
      unless ability.can?(:manage, org)
        raise Api::V1::ApiError.new(:forbidden, "You do not have permission to activate configs for this org")
      end
    end

    gen_ai_config = GenAiConfig.find(input[:gen_ai_config_id])
    if org.id != gen_ai_config.org_id && !gen_ai_config.org.nexla_admin_org?
      raise Api::V1::ApiError.new(:forbidden, "Config does not belong to the org")
    end

    usage = input[:gen_ai_usage]

    if usage.blank? || !GenAiOrgSetting.gen_ai_usages.keys.include?(usage)
      raise Api::V1::ApiError.new(:bad_request, "Invalid usage. Possible values are #{GenAiOrgSetting.gen_ai_usages.values.join(', ')}")
    end

    global = input.key?(:global) ? input[:global] : false
    if global && !api_user_info.user.super_user?
      raise Api::V1::ApiError.new(:forbidden, "Only Nexla administrators can create global settings")
    end

    if global && !org.nexla_admin_org?
      raise Api::V1::ApiError.new(:forbidden, "You do not have permissions to create a global config")
    end

    setting = nil
    GenAiOrgSetting.transaction do
      setting = GenAiOrgSetting.find_or_initialize_by(org: org, gen_ai_usage: usage, global: global)
      setting.gen_ai_config = gen_ai_config

      if usage.to_s == 'all'
        GenAiOrgSetting.where(org: org, global: global).destroy_all
      else
        GenAiOrgSetting.where(org: org, gen_ai_usage: 'all', global: global).destroy_all
      end

      setting.save!
    end
    setting
  end
end
