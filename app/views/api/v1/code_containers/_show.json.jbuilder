json.(code_container, :id, :name, :resource_type, :reusable, :public, :ai_function_type)
json.partial! @api_root + "users/owner", user: code_container.owner
json.partial! @api_root + "orgs/brief", org: code_container.org

if @access_roles[:code_containers].present?
  json.access_roles [@access_roles[:code_containers][code_container.id]]
else
  json.access_roles code_container.get_access_roles(current_user, current_org)
end

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

json.(code_container, :description, :code_type, :output_type, :code_config, :code_encoding, :repo_config)
json.code code_container.get_code
if code_container.code_error
  json.(code_container, :code_error)
end
json.(code_container, :custom_config, :managed)
json.data_sets code_container.data_sets.map(&:id)

if @tags[:code_containers].present?
  json.tags @tags[:code_containers][code_container.id]
else
  json.tags code_container.tags_list
end

if code_container.referenced_resources_enabled?
  json.referenced_resource_ids do
    code_container.referencing_fields.each do |key|
      json.set!(key, code_container.send("ref_#{key}_ids"))
    end
  end
end

json.(code_container, :copied_from_id, :updated_at, :created_at)
