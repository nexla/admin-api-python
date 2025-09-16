json.(code_container, :id, :name, :resource_type, :reusable, :public)

if (!code_container.data_credentials.nil?)
  json.data_credentials do
    if (@expand)
      json.partial! @api_root + "data_credentials/show", data_credentials: code_container.data_credentials
    else
      json.(code_container.data_credentials, :id, :name, :description, :updated_at, :created_at)
    end
  end
else
  json.data_credentials nil
end

if (!code_container.runtime_data_credentials.nil?)
  json.runtime_data_credentials do
    if (@expand)
      json.partial! @api_root + "data_credentials/show", data_credentials: code_container.runtime_data_credentials
    else
      json.(code_container.runtime_data_credentials, :id, :name, :description, :updated_at, :created_at)
    end
  end
else
  json.runtime_data_credentials nil
end

json.(code_container, :description, :code_type, :output_type, :code_config, :code_encoding, :code, :custom_config)
json.data_sets code_container.data_sets.map(&:id)
json.(code_container, :copied_from_id, :updated_at, :created_at)