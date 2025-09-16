json.(@service_key,
  :id,
  :owner_id,
  :org_id
)

if @show_dataplane_details
  json.cluster_id @service_key.org&.cluster_id
  json.cluster_uid @service_key.org&.cluster&.uid
end

json.(@service_key,
  :name,
  :description,
  :status,
  :api_key,
  :last_rotated_key,
  :last_rotated_at,
  :updated_at,
  :created_at
)
