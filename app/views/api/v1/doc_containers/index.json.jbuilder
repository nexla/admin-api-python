json.array! @doc_containers do |doc_container|
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
    :repo_config
  )

  json.text doc_container.text if (@expand && doc_container.embedded?)
  json.access_roles doc_container.get_access_roles(current_user, current_org)
  json.tags doc_container.tags_list

  json.(doc_container,
    :copied_from_id,
    :updated_at,
    :created_at
  )
end
