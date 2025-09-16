if data_credentials
  json.(data_credentials, :id, :owner_id, :org_id, :name, :managed, :vendor, :description,
    :credentials_type, :verified_status, :tags, :template_config,
    :copied_from_id, :created_at, :updated_at)
end