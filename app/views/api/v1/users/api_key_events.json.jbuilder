json.(@user, :id, :email, :full_name, :email_verified_at, :updated_at, :created_at)
json.events(@api_key_events) do |audit|
    json.(audit,
      :api_key_type,
      :resource_id,
      :scope,
      :owner_id,
      :org_id,
      :request_url,
      :request_ip,
      :request_user_agent,
      :time_of_authentication,
      :usage_count
    )
  end