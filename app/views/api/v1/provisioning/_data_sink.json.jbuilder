if data_sink
  json.(data_sink, :id, :status, :data_credentials_id, :sink_config, :connection_type, :sink_type)
  if data_sink.data_credentials
    json.data_credentials do
      json.(data_sink.data_credentials, :id, :credentials_enc, :credentials_enc_iv)
    end
  else
    json.data_credentials nil
  end
end

