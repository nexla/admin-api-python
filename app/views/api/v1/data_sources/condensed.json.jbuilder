json.array!(@data_sources) do |data_source|
  json.(data_source, :id, :origin_node_id, :flow_node_id)

  json.owner do
    json.id data_source.owner_id
  end

  json.org do
    json.id data_source.org_id
    json.cluster_id data_source.cluster_id
    json.new_cluster_id data_source.new_cluster_id
    json.cluster_status data_source.cluster_status
  end

  if (!data_source.data_credentials_id.nil?)
    json.data_credentials do
      json.id data_source.data_credentials_id
      json.credentials_enc data_source.credentials_enc
      json.credentials_enc_iv data_source.credentials_enc_iv
    end
  elsif (!data_source.code_container_id.nil?)
    json.(data_source, :code_container_id)
    json.data_credentials do
      json.id @script_credentials.id
      json.credentials_enc @script_credentials.credentials_enc
      json.credentials_enc_iv @script_credentials.credentials_enc_iv
    end
  else
    json.data_credentials nil
  end

  json.(data_source, :status, :runtime_status, :source_config, :updated_at, :created_at)
  json.source_type data_source.connection_type_raw
  json.connector_type data_source.connector_type
  json.flow_type data_source.flow_type_raw
  json.ingestion_mode data_source.ingestion_mode_raw

  json.flow_triggers do
    json.array! @triggers[data_source.origin_node_id] do |trigger|
      json.(trigger, :triggering_flow_node_id)
      json.triggering_resource_type "data_sink"
      json.triggering_resource_id trigger.triggering_resource.id
    end
  end
  
  if (!data_source.code_config.nil?)
    json.script_config JSON.parse(data_source.code_config)
  end
end
