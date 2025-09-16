json.(data_credentials_group, :id, :name, :description, :credentials_type)

json.data_credentials_count data_credentials_group.data_credentials&.count || 0

json.partial! @api_root + "users/owner", user: data_credentials_group.owner
json.partial! @api_root + "orgs/brief", org: data_credentials_group.org

json.(data_credentials_group, :created_at, :updated_at)
