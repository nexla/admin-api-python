json.array!(@requests) do |request|
  json.(request, :id, :status, :email, :full_name, :created_user_id, :created_org_id, :invite_id, :created_at, :updated_at)
end