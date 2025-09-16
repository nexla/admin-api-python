json.(data_source, :id, :origin_node_id, :flow_node_id)
json.(data_source.flow_node, :parent_node_id)

json.partial! @api_root + "users/owner", user: data_source.owner
json.partial! @api_root + "orgs/brief", org: data_source.org

if @access_roles[:data_sources].present?
  json.access_roles [@access_roles[:data_sources][data_source.id]]
else
  json.access_roles data_source.get_access_roles(current_user, current_org)
end

json.(data_source,
  :name,
  :description,
  :status, :runtime_status)

json.data_sets(data_source.data_sets) do |s|
  json.version s.get_latest_version
  json.(s, :id, :owner_id, :org_id, :name, :description, :updated_at, :created_at)
  if (@expand)
    json.(s, :sample_service_id, :source_schema, :transform)
    json.output_schema s.output_schema_with_annotations
    json.source_config s.data_source&.source_config
    json.connector_type s.data_source&.connector_type
    json.partial! @api_root + "connectors/show", connector: s.connector
  end
end

json.(data_source,
  :ingest_method,
  :source_format,
  :source_config,
  :poll_schedule,
  :managed,
  :adaptive_flow,
  :code_container_id)

json.source_type data_source.raw_source_type(current_user)
json.connector_type data_source.connector_type
json.partial! @api_root + "connectors/show", connector: data_source.connector

json.api_keys(data_source.api_keys) do |k|
  if data_source.flow_type == FlowNode::Flow_Types[:rag]
    json.(k, *k.attributes.keys)
    json.url data_source.ai_web_server_url
  elsif data_source.flow_type == FlowNode::Flow_Types[:api_server]
    json.(k, *k.attributes.keys)
    json.url data_source.api_web_server_url
  else
    json.(k,
      :id,
      :owner_id,
      :org_id,
      :data_source_id,
      :name,
      :description,
      :status,
      :scope,
      :api_key,
      :url,
      :last_rotated_key,
      :last_rotated_at,
      :updated_at,
      :created_at
    )
  end
end

json.auto_generated data_source.auto_generated

if data_source.auto_generated && data_source.data_sink.present?
  json.data_sink do
    json.partial! @api_root + "data_sinks/brief", data_sink: data_source.data_sink
  end
end

if (!data_source.data_credentials.nil?)
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: data_source.data_credentials
  end
elsif (data_source.script_enabled? && !data_source.script_data_credentials.nil? && current_user.infrastructure_user?)
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: data_source.script_data_credentials
  end
else
  json.data_credentials nil
end

json.(data_source, :data_credentials_group_id)
json.run_profile(data_source.run_profile) if data_source.adaptive_flow?
json.run_variables(data_source.run_variables) if data_source.adaptive_flow?

if data_source.has_template?
  json.vendor_endpoint do
    json.partial! @api_root + "vendor_endpoints/brief", vendor_endpoint: data_source.vendor_endpoint
  end
  if (!data_source.vendor_endpoint.nil? && !data_source.vendor_endpoint.vendor.nil?)
    json.vendor do
      json.partial! @api_root + "vendors/brief", vendor: data_source.vendor_endpoint.vendor
    end
  else
    json.vendor nil
  end
  json.(data_source, :template_config)
end

json.script_config data_source.code_container.code_config if !data_source.code_container.nil?

runs = @run_ids.present? ? @run_ids[data_source.id] : data_source.runs
json.run_ids(runs) do |run|
  json.id run.run_id
  json.created_at run.created_at
end

json.summary data_source.summary if (@include_summary)

if @tags[:data_sources].present?
  json.tags @tags[:data_sources][data_source.id]
else
  json.tags data_source.tags_list
end

if data_source.referenced_resources_enabled?
  json.referenced_resource_ids do
    data_source.referencing_fields.each do |key|
      json.set!(key, data_source.send("ref_#{key}_ids"))
    end
  end
end

json.(data_source, :endpoint_mappings) if data_source.flow_type == FlowNode::Flow_Types[:api_server]

json.(data_source, :flow_type, :ingestion_mode, :last_run_id)
json.flow_triggers(data_source.flow_triggers) do |flow_trigger|
  json.partial! @api_root + "flow_triggers/show", flow_trigger: flow_trigger
end
json.linked_flows data_source.origin_node&.linked_flow_ids
json.(data_source, :copied_from_id, :updated_at, :created_at)
