json.(data_set, :id, :origin_node_id, :flow_node_id)
json.(data_set.flow_node, :parent_node_id)

json.owner do
  json.(data_set.owner, :id, :full_name, :email)
end

json.org do
  if (data_set.org.nil?)
    json.nil!
  else
    json.(data_set.org, :id, :name, :email_domain)
  end
end

json.(data_set, :name, :description, :status, :runtime_status, :public, :data_credentials_id)

if @access_roles[:data_sets].present?
  json.access_roles [@access_roles[:data_sets][data_set.id]]
else
  json.access_roles data_set.get_access_roles(current_user, current_org)
end

json.output_schema data_set.output_schema_with_annotations
json.nexset_api_config data_set.get_nexset_api_config
parent_source = data_set.parent_source
json.source_config parent_source&.source_config
json.connector_type parent_source&.connector_type
json.partial! @api_root + "connectors/show", connector: data_set.connector

json.(data_set, :semantic_schema_id)
json.semantic_schema data_set.semantic_schema&.schema

json.(data_set, :data_sample_id)
if (@include_samples)
  json.data_samples data_set.prepare_data_samples_with_metadata(data_set.data_samples)
end

json.(data_set, :updated_at)
if @tags[:data_sets].present?
  json.tags @tags[:data_sets][data_set.id]
else
  json.tags data_set.tags_list
end

if data_set.org&.marketplace_enabled?
  json.domains data_set.domains do |domain|
    json.(domain, :id, :name)
  end
end
