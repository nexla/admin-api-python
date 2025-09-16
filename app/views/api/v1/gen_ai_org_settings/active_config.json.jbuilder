json.(@config, :id, :name, :status, :type, :description, :config, :owner_id, :org_id, :gen_ai_config_source)

if can?(:read, @config)
  json.data_credentials do
    json.partial! partial: @api_root + 'data_credentials/show', data_credentials: @config.data_credentials
  end
else
  json.data_credentials do
    json.hidden true
  end
end