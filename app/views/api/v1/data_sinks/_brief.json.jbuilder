json.(data_sink, :id, :origin_node_id, :flow_node_id)
json.partial! @api_root + "users/owner", user: data_sink.owner
json.partial! @api_root + "orgs/brief", org: data_sink.org

if @access_roles[:data_sinks].present?
  json.access_roles [@access_roles[:data_sinks][data_sink.id]]
else
  json.access_roles data_sink.get_access_roles(current_user, current_org)
end

json.(data_sink,
  :name,
  :description,
  :sink_format,
  :in_memory,
  :status,
  :runtime_status,
  :copied_from_id,
  :updated_at,
  :created_at)

json.sink_type data_sink.raw_sink_type(current_user)
json.connector_type data_sink.connector_type
json.partial! @api_root + "connectors/show", connector: data_sink.connector
