json.(data_credentials, :id)
json.(data_credentials, :name, :description)

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

if @access_roles[:data_credentials].present?
  json.access_roles [@access_roles[:data_credentials][data_credentials.id]]
else
  json.access_roles data_credentials.get_access_roles(current_user, current_org, false)
end

json.(data_credentials, :credentials_version)
json.credentials_type data_credentials.raw_credentials_type(current_user)
json.partial! @api_root + "connectors/show", connector: data_credentials.connector

if current_user.infrastructure_user?
  json.(data_credentials, :credentials_enc, :credentials_enc_iv)
end

if data_credentials.has_template?
  json.vendor do
    json.partial! @api_root + "vendors/brief", vendor: data_credentials.vendor
  end
  json.(data_credentials, :template_config)
end

json.(data_credentials,
  :verified_status,
  :verified_at,
  :copied_from_id,
  :updated_at,
  :created_at
)
