json.array! @service_keys do |service_key|
  json.(service_key,
    :id,
    :owner_id,
    :org_id,
    :name,
    :description,
    :status,
    :api_key,
    :last_rotated_key,
    :last_rotated_at,
    :updated_at,
    :created_at
  )
end