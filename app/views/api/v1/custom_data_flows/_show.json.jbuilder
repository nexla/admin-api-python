json.(custom_data_flow, :id)
json.partial! @api_root + "users/owner", user: custom_data_flow.owner
json.partial! @api_root + "orgs/brief", org: custom_data_flow.org

json.(custom_data_flow,
  :name,
  :description,
  :flow_type,
  :status,
  :managed,
  :config
)

json.code_containers custom_data_flow.code_containers do |code_container|
  json.partial! @api_root + 'code_containers/show', code_container: code_container
end

json.data_credentials custom_data_flow.data_credentials do |dc|
  json.partial! @api_root + "data_credentials/show", data_credentials: dc
end

json.access_roles custom_data_flow.get_access_roles(current_user, current_org)
json.tags custom_data_flow.tags_list

json.(custom_data_flow,
  :copied_from_id,
  :updated_at,
  :created_at
)
