module TokenHelper

  def audit_user_login(token_hash)
    UserLoginAudit.create(request_info.merge({
      time_of_issue: Time.now,
      time_of_expiration: Time.now + token_hash[:expires_in],
      token_key: token_disgest(token_hash[:access_token]),
      audit_type: User::AUTHENTICATION_TYPES[:login]
    }))
    if token_hash[:skey]
      service_key = ServiceKey.find_by(external_id: token_hash[:skey])
      audit_service_key service_key
    end
  end

  def audit_user_logout token
    UserLoginAudit.create(request_info.merge({
      time_of_invalidation: Time.now,
      token_key: token_disgest(token),
      audit_type: User::AUTHENTICATION_TYPES[:logout]
    }))
  end

  def request_info
    {
      request_url: request.url,
      request_user_agent: request.user_agent.to_s.downcase,
      request_ip: request.remote_ip,
      user_id: current_user.id,
      org_id: (current_org.present? ? current_org.id : nil)
    }
  end

end