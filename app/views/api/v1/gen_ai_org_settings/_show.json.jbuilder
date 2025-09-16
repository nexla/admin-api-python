json.(gen_ai_org_setting, :id)
json.(gen_ai_org_setting, :org_id)
json.(gen_ai_org_setting, :gen_ai_usage)
json.(gen_ai_org_setting, :gen_ai_config_id)

if gen_ai_org_setting.global?
  json.(gen_ai_org_setting, :global)
end

if current_user.super_user? || gen_ai_org_setting.org.has_collaborator_access?(current_user)
  json.gen_ai_config do
    json.partial! @api_root + "gen_ai_integration_configs/show", gen_ai_config: gen_ai_org_setting.gen_ai_config
  end
end