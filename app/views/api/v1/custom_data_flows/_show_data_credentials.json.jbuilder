json.array!(@custom_data_flow.data_credentials) do |data_credentials|
  json.partial! @api_root + 'data_credentials/show', data_credentials: data_credentials
end
