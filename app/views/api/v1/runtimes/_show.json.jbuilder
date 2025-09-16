json.(runtime, :id)
json.partial! @api_root + "users/owner", user: runtime.owner
json.partial! @api_root + "orgs/brief", org: runtime.org

json.(runtime, :name, :description, :active, :dockerpath, :managed, :config)

if runtime.data_credentials.present?
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: runtime.data_credentials
  end
end

json.(runtime, :created_at, :updated_at)