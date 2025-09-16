json.array! @data_credentials do |data_credentials|

  json.(data_credentials,
    :id,
    :name,
    :description,
    :credentials_version,
    :verified_status,
    :verified_at,
    :copied_from_id,
    :updated_at,
    :created_at
  )
  json.credentials_type data_credentials.raw_credentials_type(current_user)
  json.partial! @api_root + "connectors/show", connector: data_credentials.connector

  json.owner do
    json.(data_credentials.owner, :id, :full_name, :email)
  end

  json.org do
    if (data_credentials.org.nil?)
      json.nil!
    else
      json.(data_credentials.org, :id, :name, :email_domain, :email, :client_identifier)
    end
  end

  if current_user.infrastructure_user?
    json.(data_credentials, :credentials_enc, :credentials_enc_iv)
  end

end

