json.(catalog_config, :id)
json.partial! @api_root + "users/owner", user: catalog_config.owner
json.partial! @api_root + "orgs/brief", org: catalog_config.org

if (!catalog_config.data_credentials.nil?)
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: catalog_config.data_credentials
  end
else
  json.data_credentials nil
end

json.(catalog_config,
  :name,
  :description,
  :status,
  :config,
  :templates,
  :mode,
  :job_id,
  :updated_at,
  :created_at
)
