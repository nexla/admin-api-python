json.(data_schema, :id)
json.partial! @api_root + "users/owner", user: data_schema.owner
json.partial! @api_root + "orgs/brief", org: data_schema.org
json.version data_schema.latest_version
json.(data_schema, :name, :description)
json.access_roles data_schema.get_access_roles(current_user, current_org)

json.(data_schema, :name, :description, :detected, :managed, :template, :public,
  :schema, :annotations, :validations, :data_samples)

if (@expand)
  json.data_sets data_schema.data_sets do |ds|
    json.partial! @api_root + 'data_sets/show', data_set: ds
  end
else
  json.data_sets data_schema.data_sets.map(&:id)
end

json.tags data_schema.tags_list
json.(data_schema, :updated_at, :created_at)
