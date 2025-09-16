json.array! @api_keys do |api_key|
  json.(api_key,
    :id,
    :owner_id,
    :org_id,
    @resource_attribute,
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
