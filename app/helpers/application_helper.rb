module ApplicationHelper

  def initialize_date_params (default_interval = nil)
    if (params[:interval].present? || 
      params[:from].present? || 
      params[:to].present?)
      @date_interval = DateInterval.new(params[:interval], params[:from], params[:to])
    else
      @date_interval = DateInterval.new(default_interval)
    end
    params[:from] = @date_interval.start_time
    params[:to] = @date_interval.end_time
  end

  def audit_service_key(service_key)
    return if service_key.blank?
    event = ServiceKeyEvent.where({
      request_ip: request.remote_ip,
      service_key_id: service_key.id,
      time_of_authentication: Time.now.all_day
    }).first

    if event.present?
      event.update(usage_count: event.usage_count + 1)
    else
      ServiceKeyEvent.create({
        request_url: request.url,
        request_user_agent: request.user_agent.to_s.downcase,
        request_ip: request.remote_ip,
        owner_id: service_key.owner_id,
        org_id: service_key.org_id,
        service_key_id: service_key.id,
        service_key_api_key: service_key.api_key,
        time_of_authentication: Time.now,
        usage_count: 1
      })
    end
  end

  def audit_api_key(api_key)
    return if api_key.blank?

    user_agent = request&.user_agent
    if api_key.owner.nexla_backend_admin?
      # See NEX-129999. We add some extra logging here
      # to make it easier to find backend api-key uses
      # that might otherwise get cycled out of the 
      # api_key_events table by the 100-events-per
      # user limit that we apply in Nexla prod.
      # This can be removed once we have identified
      # and fixed all such callers and removed the
      # corresponding users_api_keys entries.
      org = api_key.org
      key = api_key.api_key
      masked_part = '*' * (key.length - 8)
      masked_key = "#{key[0, 4]}#{masked_part}#{key[-4, 4]}"
      Rails.logger.warn("[Backend Admin Login] login with Database API key: #{masked_key}, user_agent: #{user_agent}, org_id #{org&.id || 'nil'}")
    end

    return unless api_key.key_events_enabled?

    event = ApiKeyEvent.where({
      request_ip: request.remote_ip, 
      api_key_id: api_key.id, 
      api_key_type: api_key.class.resource_attribute, 
      time_of_authentication: Time.now.all_day
    }).first
    
    if event.present?
      event.update(usage_count: event.usage_count + 1)
    else
      ApiKeyEvent.create({
        request_url: request.url,
        request_user_agent: request.user_agent.to_s.downcase,
        request_ip: request.remote_ip,
        owner_id: api_key.owner_id,
        org_id: api_key.org_id,
        scope: api_key.scope,
        api_key_type: api_key.class.resource_attribute,
        api_key_id: api_key.id,
        api_key_api_key: api_key.api_key,
        resource_id: api_key.resource_instance.id,
        time_of_authentication: Time.now,
        usage_count: 1
      }) 
    end
  end

end
