json.resource_type @resource_type
json.resource_id @resource.id

json.event_type @event_type
json.resource_json do
  if @resource.class.name == 'DataSet'
    json.partial! @api_root + "data_sets/show", data_set: @resource
  elsif @resource.class.name == 'DataSource'
    json.partial! @api_root + "data_sources/show", data_source: @resource
  elsif  @resource.class.name == 'DataSink'
    json.partial! @api_root + "data_sinks/show", data_sink: @resource
  elsif @resource.class.name == 'DataCredentials'
    json.partial! @api_root + "data_credentials/show", data_credentials: @resource
  elsif @resource.class.name == 'NotificationSetting'
    json.partial! @api_root + "notification_settings/show", notification_setting: @resource
  elsif @resource.class.name == 'Org'
    json.partial! @api_root + "orgs/show", org: @resource
  end
end

if @resource.class.name == 'FlowNode' && @resource.origin_node.data_source.present?
  json.source_json do
    json.partial! @api_root + "data_sources/show", data_source: @resource.origin_node.data_source
  end
end

if @resource.respond_to?(:origin_node)
  json.flow do
    json.(@resource.origin_node, :id, :shared_origin_node_id, :owner_id, :org_id, :cluster_id,
        :name, :description, :status, :ingestion_mode, :flow_type, :project_id,
        :data_source_id, :data_set_id, :data_sink_id, :nexset_api_compatible,
        :managed, :copied_from_id, :created_at, :updated_at
    )
  end
  json.flow_id @resource.origin_node_id
end

if @event_type == :schema_update && @resource.respond_to?(:output_schema)
  json.schema @resource.output_schema
end

if @resource.is_a?(DataSource) || @resource.is_a?(DataSink)
  json.connection_type @resource.connector.connection_type
elsif @resource.is_a?(DataSet) && @resource.data_source.present?
  json.connection_type @resource.data_source.connector.connection_type
end

if @event_type == :update && @resource.respond_to?(:change_list) && @resource.change_list.present?
  json.update_details do
    json.updated_fields @resource.change_list.keys
    json.before @resource.change_list.transform_values(&:first)
    json.after @resource.change_list.transform_values(&:last)
  end
end