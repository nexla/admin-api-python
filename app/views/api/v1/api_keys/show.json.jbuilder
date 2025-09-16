json.(@api_key,
  :id,
  @resource_attribute,
  :owner_id,
  :org_id
)

if @show_dataplane_details
  json.cluster_id @api_key.org&.cluster_id
  json.cluster_uid @api_key.org&.cluster&.uid
end

json.(@api_key,
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
