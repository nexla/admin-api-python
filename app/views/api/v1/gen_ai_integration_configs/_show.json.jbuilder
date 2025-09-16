json.(gen_ai_config, :id)
json.partial! @api_root + "users/owner", user: gen_ai_config.owner
json.partial! @api_root + "orgs/brief", org: gen_ai_config.org

json.data_credentials do
  json.partial! @api_root + "data_credentials/show", data_credentials: gen_ai_config.data_credentials
end

json.(gen_ai_config,
  :name, :description, :status, :config,
  :type, :updated_at, :created_at)
