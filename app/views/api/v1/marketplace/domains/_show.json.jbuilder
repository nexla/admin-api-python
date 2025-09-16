json.(domain,
  :id,
  :parent_id,
  :org_id,
  :name,
  :description,
  :items_count,
  :created_at,
  :updated_at
)

json.owner do
  json.(domain.owner, :id, :full_name, :email)
end
