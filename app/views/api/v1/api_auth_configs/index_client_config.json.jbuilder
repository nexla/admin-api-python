json.array! @api_auth_configs do |auth_config|
  json.(auth_config, :id, :name, :client_config)
end