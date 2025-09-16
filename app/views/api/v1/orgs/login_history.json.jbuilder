json.(@org,
  :id,
  :name,
  :description,
  :email_domain,
  :email
)
json.login_history(@login_history) do |audit|
  json.(audit,
    :user_id,
    :request_url,
    :request_ip,
    :request_user_agent,
    :time_of_issue,
    :time_of_expiration
  )
end
json.logout_history(@logout_history) do |audit|
  json.(audit,
    :user_id,
    :request_url,
    :request_ip,
    :request_user_agent,
    :time_of_invalidation
  )
end