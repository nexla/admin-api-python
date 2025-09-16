json.(data_credentials, :id)
json.(data_credentials, :name, :description)

json.owner_id data_credentials.owner_id
json.org_id data_credentials.org_id

if @access_roles[:data_credentials].present?
  json.access_roles @access_roles[:data_credentials][data_credentials.id]
else
  json.access_roles data_credentials.get_access_roles(current_user, current_org)
end

json.(data_credentials, :credentials_version, :managed)
json.credentials_type data_credentials.raw_credentials_type(current_user)
json.connector_type data_credentials.connector_type

json.api_key_id data_credentials.users_api_key_id

json.(data_credentials, :credentials_non_secure_data)

if current_user.infrastructure_user?
  json.(data_credentials, :credentials_enc, :credentials_enc_iv)
end

if data_credentials.has_template?

  json.vendor do
    json.(data_credentials.vendor, :id, :name, :display_name)
  end

  if !data_credentials.auth_template.nil?
    json.auth_template do
      json.(data_credentials.auth_template, :id, :name, :display_name)
    end
  end

  json.(data_credentials, :template_config)
end

json.(data_credentials, :verified_status, :verified_at, :copied_from_id, :updated_at, :created_at)
if @tags[:data_credentials].present?
  json.tags @tags[:data_credentials][data_credentials.id]
else
  json.tags data_credentials.tags_list
end