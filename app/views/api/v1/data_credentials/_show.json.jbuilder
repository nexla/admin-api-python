json.(data_credentials, :id)
json.(data_credentials, :name, :description)

json.partial! @api_root + "users/owner", user: data_credentials.owner
json.partial! @api_root + "orgs/brief", org: data_credentials.org

if @access_roles[:data_credentials].present?
  json.access_roles [@access_roles[:data_credentials][data_credentials.id]]
else
  json.access_roles data_credentials.get_access_roles(current_user, current_org)
end

json.(data_credentials, :credentials_version, :managed)
json.credentials_type data_credentials.raw_credentials_type(current_user)
json.partial! @api_root + "connectors/show", connector: data_credentials.connector

# Note, we format this as an array of keys even though
# DataCredentials has at most one associated api key to
# keep the response consistent with other resource
# api key responses.
json.api_keys data_credentials.api_keys do |api_key|
  json.(api_key,
    :id,
    :owner_id,
    :org_id,
    :user_id,
    :name,
    :description,
    :status,
    :scope,
    :api_key,
    :url,
    :last_rotated_key,
    :last_rotated_at,
    :updated_at,
    :created_at
  )
end

json.(data_credentials, :credentials_non_secure_data)

if current_user.infrastructure_user?
  json.(data_credentials, :credentials_enc, :credentials_enc_iv)
end

if data_credentials.has_template?
  json.vendor do
    if @expand
      json.partial! @api_root + "vendors/show", vendor: data_credentials.vendor
    else
      json.(data_credentials.vendor, :id, :name, :display_name)
    end
  end
  json.(data_credentials, :template_config)

  if !data_credentials.auth_template.nil?
    json.auth_template do
      if @expand
        json.partial! @api_root + "auth_templates/show", auth_template: data_credentials.auth_template
      else
        json.(data_credentials.auth_template, :id, :name, :display_name)
      end
    end
  else
    json.auth_template({})
  end
end

if @tags[:data_credentials].present?
  json.tags @tags[data_credentials.id]
else
  json.tags data_credentials.tags_list
end

if data_credentials.referenced_resources_enabled?
  json.referenced_resource_ids do
    data_credentials.referencing_fields.each do |key|
      json.set!(key, data_credentials.send("ref_#{key}_ids"))
    end
  end
end

json.(data_credentials, 
  :verified_status,
  :verified_at,
  :copied_from_id,
  :updated_at,
  :created_at
)

