json.(data_source, :id, :origin_node_id, :flow_node_id)

json.owner do
  json.(data_source.owner, :id, :full_name, :email)
end
json.org do
  if (data_source.org.nil?)
    json.nil!
  else
    json.(data_source.org, :id, :name, :email_domain, :email, :client_identifier)
  end
end

json.(data_source,
  :name,
  :description,
  :status,
  :runtime_status)

json.data_sets(data_source.data_sets) do |s|
  json.(s, :id, :owner_id, :org_id, :name, :description, :updated_at, :created_at)
end

json.(data_source,
  :ingest_method,
  :source_format,
  :source_config,
  :poll_schedule,
  :managed,
  :auto_generated,
  :code_container_id)

json.source_type data_source.raw_source_type(current_user)
json.connector_type data_source.connector_type
json.partial! @api_root + "connectors/show", connector: data_source.connector

if data_source.auto_generated && data_source.data_sink.present?
  json.data_sink do
    json.(data_source.data_sink, :id, :name, :description, :sink_format, :status, :runtime_status, :updated_at, :created_at)
    json.sink_type data_source.data_sink.raw_sink_type(current_user)
    json.partial! @api_root + "connectors/show", connector: data_source.data_sink.connector
  end
end

if (!data_source.data_credentials.nil?)
  json.data_credentials do
    json.(data_source.data_credentials, :id, :name, :credentials_version)
    json.credentials_type data_source.data_credentials.raw_credentials_type(current_user)
    json.partial! @api_root + "connectors/show", connector: data_source.data_credentials.connector
    if current_user.infrastructure_user?
      json.(data_source.data_credentials, :credentials_non_secure_data, :credentials_enc, :credentials_enc_iv)
    end
  end
elsif (data_source.script_enabled? && !data_source.script_data_credentials.nil? && current_user.infrastructure_user?)
  json.data_credentials do
    json.(data_source.script_data_credentials, :id, :name, :credentials_version, :credentials_non_secure_data, :credentials_enc, :credentials_enc_iv)
    json.credentials_type data_source.script_data_credentials.raw_credentials_type(current_user)
    json.partial! @api_root + "connectors/show", connector: data_source.script_data_credentials.connector
  end
else
  json.data_credentials nil
end

json.(data_source, :data_credentials_group_id)

if data_source.has_template?
  json.vendor_endpoint do
    json.(data_source.vendor_endpoint, :id, :name, :display_name)
  end
  json.(data_source, :template_config)
end

json.script_config data_source.code_container.code_config if !data_source.code_container.nil?

json.flow_type data_source.flow_type
json.ingestion_mode data_source.ingestion_mode

json.(data_source,
  :copied_from_id,
  :updated_at,
  :created_at)
