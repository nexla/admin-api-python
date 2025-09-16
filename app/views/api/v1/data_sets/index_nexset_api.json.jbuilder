json.array! @data_sets do |data_set|
  json.(data_set, :id, :origin_node_id, :flow_node_id)
  json.owner do
    json.(data_set.owner, :id, :full_name, :email)
  end
  if data_set.org.nil?
    json.org nil
  else
    json.org do
      json.(data_set.org, :id, :name, :email_domain, :client_identifier)
    end
  end

  json.(data_set,
    :name,
    :description,
    :status,
    :data_credentials_id,
    :runtime_status,
    :public,
    :managed,
    :data_source_id,
    :nexset_api_config
  )
  
  # FN backwards compatibility (NEX-9253)
  json.sync_api_config data_set.nexset_api_config

  json.parent_data_sets [data_set.parent_data_set].compact do |pds|
    json.(pds, :id, :owner_id, :org_id, :name, :description)
  end

  json.data_sinks data_set.data_sinks do |ds|
    json.(ds, :id, :owner_id, :org_id, :name, :description)
  end

  json.transform_id data_set.code_container_id
  json.(data_set, :copied_from_id, :created_at, :updated_at)

  if @tags[:data_sets].present?
    json.tags @tags[:data_sets][data_set.id]
  else
    json.tags data_set.tags_list
  end
end
