json.logout_history(audits) do |audit|
  json.(audit,
    :request_url,
    :request_ip,
    :request_user_agent,
    :time_of_invalidation
  )
end