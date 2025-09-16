json.(doc_container, :id)

json.owner do
  json.(doc_container.owner, :id, :full_name, :email)
end

if doc_container.org.nil?
  json.org nil
else
  json.org do
    json.(doc_container.org, :id, :name, :email_domain, :client_identifier)
  end
end

json.(doc_container,
  :name,
  :description,
  :doc_type,
  :public,
  :repo_type,
  :repo_config,
  :text
)

if (!doc_container.data_credentials.nil?)
  json.data_credentials do
    if (@expand)
      json.partial! @api_root + "data_credentials/show", data_credentials: doc_container.data_credentials
    else
      json.(doc_container.data_credentials, :id, :name, :description, :updated_at, :created_at)
    end
  end
else
  json.data_credentials nil
end

json.access_roles doc_container.get_access_roles(current_user, current_org)
json.tags doc_container.tags_list

json.(doc_container,
  :copied_from_id,
  :updated_at,
  :created_at
)
