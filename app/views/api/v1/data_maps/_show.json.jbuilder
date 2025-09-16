json.(data_map, :id)
json.partial! @api_root + "users/owner", user: data_map.owner
json.partial! @api_root + "orgs/brief", org: data_map.org

if @access_roles[:data_maps].present?
  json.access_roles [@access_roles[:data_maps][data_map.id]]
else
  json.access_roles data_map.get_access_roles(current_user, current_org)
end

json.(data_map, :name, :description, :public, :managed)
json.(data_map, :data_type, :data_format, :data_sink_id)
json.data_set_id data_map.data_set&.id
json.(data_map, :emit_data_default, :use_versioning, :map_primary_key, :data_defaults)
json.(data_map, :data_map) if (@expand)
json.map_entry_count data_map.get_map_entry_count(true)
json.map_entry_info data_map.get_map_validation if @validate
json.map_entry_schema data_map.get_map_entry_schema

if @tags[:data_maps].present?
  json.tags @tags[:data_maps][data_map.id]
else
  json.tags data_map.tags_list
end

json.(data_map, :copied_from_id, :updated_at, :created_at)
