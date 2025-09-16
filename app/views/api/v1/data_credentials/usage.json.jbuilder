
json.(@data_credentials,
  :id,
  :owner_id,
  :org_id,
  :connector_type,
  :users_api_key_id,
  :name,
  :description,
  :vendor_id,
  :auth_template_id,
  :verified_status,
  :verified_at,
  :copied_from_id,
  :created_at,
  :updated_at
)

json.data_sources(@data_sources) do |data_source|
  json.(data_source, *@attrs)
end

json.data_sinks(@data_sinks) do |data_sink|
  json.(data_sink, *@attrs)
end

json.quarantine_settings(@quarantine_settings) do |quarantine_setting|
  json.(quarantine_setting, *@quarantine_attrs)
end
