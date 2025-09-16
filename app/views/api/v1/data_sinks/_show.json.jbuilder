json.(data_sink, :id, :origin_node_id, :flow_node_id)
json.(data_sink.flow_node, :parent_node_id)

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
  :status,
  :runtime_status,
  :data_set_id,
  :data_map_id,
  :data_source_id,
  :sink_format,
  :sink_config,
  :sink_schedule,
  :in_memory,
  :managed
)

json.sink_type data_sink.raw_sink_type(current_user)
json.connector_type data_sink.connector_type
json.partial! @api_root + "connectors/show", connector: data_sink.connector

if (!data_sink.data_map.nil?)
  # NEX-4912 Don't pass @expand on to data_maps render when
  # rendering from within a dynamic sink. @expand == true
  # causes an expensive infrastructure call to get the map entry
  # count, which can impact performance of /data_sets and
  # /data_sinks renders when there is a dynamic map.
  save_expand = @expand
  @expand = false
  json.data_map do
    json.partial! @api_root + "data_maps/show", data_map: data_sink.data_map
  end
  @expand = save_expand
end

if (@expand and !data_sink.data_set.nil?)
  json.data_set do
    json.(data_sink.data_set, :id, :origin_node_id, :flow_node_id, :name, :description)
    json.output_schema data_sink.data_set.output_schema_with_annotations
    json.source_config data_sink.data_source&.source_config
    json.connector_type data_sink.data_source&.connector_type
    json.partial! @api_root + "connectors/show", connector: data_sink.data_source&.connector
    json.(data_sink.data_set, :status, :updated_at, :created_at)
    json.version data_sink.data_set.get_latest_version
  end
elsif !data_sink.data_set.nil?
  json.data_set do
    json.(data_sink.data_set, :id, :name)
  end
end

if (!data_sink.data_credentials.nil?)
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: data_sink.data_credentials
  end
elsif (data_sink.script_enabled? && !data_sink.script_data_credentials.nil? && current_user.infrastructure_user?)
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: data_sink.script_data_credentials
  end
else
  json.data_credentials nil
end

json.(data_sink, :data_credentials_group_id)
json.run_variables(data_sink.run_variables) if data_sink.adaptive_flow?

if data_sink.has_template?
  json.vendor_endpoint do
    json.partial! @api_root + "vendor_endpoints/brief", vendor_endpoint: data_sink.vendor_endpoint
  end
  if (!data_sink.vendor_endpoint.nil? && !data_sink.vendor_endpoint.vendor.nil?)
    json.vendor do
      json.partial! @api_root + "vendors/brief", vendor: data_sink.vendor_endpoint.vendor
    end
  else
    json.vendor nil
  end
  json.(data_sink, :template_config)
end

json.script_config data_sink.code_container.code_config if !data_sink.code_container.nil?

if @tags[:data_sinks].present?
  json.tags @tags[:data_sinks][data_sink.id]
else
  json.tags data_sink.tags_list
end

if data_sink.referenced_resources_enabled?
  json.referenced_resource_ids do
    data_sink.referencing_fields.each do |key|
      json.set!(key,  data_sink.send("ref_#{key}_ids"))
    end
  end
end

json.(data_sink, :flow_type, :ingestion_mode)
json.flow_triggers(data_sink.flow_triggers) do |flow_trigger|
  json.partial! @api_root + "flow_triggers/show", flow_trigger: flow_trigger
end
json.(data_sink, :copied_from_id, :updated_at, :created_at)
