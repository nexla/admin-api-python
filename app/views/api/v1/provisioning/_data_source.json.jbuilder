if data_source
  json.(data_source, :id, :status, :source_config, :connection_type, :source_type)
  if data_source.data_credentials
    json.data_credentials do
      json.(data_source.data_credentials, :id, :credentials_enc, :credentials_enc_iv)
    end
  else
    json.data_credentials nil
  end
  if data_source.data_sink.present?
    json.data_sink do
      json.id data_source.data_sink_id
    end
  end
end