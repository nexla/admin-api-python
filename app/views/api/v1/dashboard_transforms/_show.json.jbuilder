json.(dashboard_transform, :id)
json.partial! @api_root + "users/owner", user: dashboard_transform.owner
json.partial! @api_root + "orgs/brief", org: dashboard_transform.org

json.(dashboard_transform,
    :resource_type,
    :resource_id)

json.error_transform do
  json.partial! @api_root + "code_containers/show", code_container: dashboard_transform.code_container
end