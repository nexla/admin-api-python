if catalog_config
  json.catalog_config do
    json.(catalog_config, :id, :name, :status, :config)

    if catalog_config.data_credentials
      json.data_credentials do
        json.(catalog_config.data_credentials, :id, :name, :credentials_type, :credentials_enc, :credentials_enc_iv,
          :verified_status, :verified_at, :copied_from_id, :created_at, :updated_at)

        if catalog_config.data_credentials&.connector
          json.connector do
            json.(catalog_config.data_credentials.connector, :id, :name, :type, :connection_type, :description, :nexset_api_compatible)
          end
        end
      end
    else
      json.data_credentials nil
    end

  end
else
  json.catalog_config nil
end