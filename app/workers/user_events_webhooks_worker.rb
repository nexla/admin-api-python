class UserEventsWebhooksWorker
  include Sidekiq::Worker

  def perform(event_name, event_data)
    return unless ENV['ALLOWED_ORIGINS'].present?

    case event_name
    when 'user_created' then user_created(event_data)
    when 'trial_expired' then trial_expired(event_data)
    when 'approval_pending' then approval_pending(event_data)
    end
  end

  private
  def user_created(event_data)
    return unless ENV['ACCOUNT_EVENT_HOOK_URL'].present?

    user = User.find_by(id: event_data['user_id'])
    org = Org.find_by(id: event_data['org_id'])

    payload = {
      event_type: "nexla_user_created",
      message: "New user added to Nexla",
      user_acq_type: event_data["acquisition_type"],
      env: get_env_name,
      user: {
       id: user.id,
       email: user.email,
       full_name: user.full_name
     },
     personal_info: { }
    }

    if org.present?
      org = Org.find_by(id: org)
      payload[:org] = {
        id: org.id,
        name: org.name
      }

      payload[:personal_info] = org.org_additional_info.data if org.org_additional_info&.data
    end

    send_data(payload, ENV['ACCOUNT_EVENT_HOOK_URL'])
  end

  def trial_expired(event_data)
    return unless ENV['ACCOUNT_EVENT_HOOK_URL'].present?

    user = User.find_by(id: event_data['user_id'])
    org = Org.find_by(id: event_data['org_id'])
    payload = {
      event_type: "nexla_org_trial_expired",
      message: "Org Trial Expired",
      env: get_env_name,
      org: {
        id: org.id,
        name: org.name
      },
       user: {
         id: user.id,
         email: user.email,
         full_name: user.full_name
       }
    }

    send_data(payload, ENV['ACCOUNT_EVENT_HOOK_URL'])
  end

  def approval_pending(event_data)
    return if ENV['APPROVAL_EVENT_HOOK_URL'].blank?

    payload = {
      event_type: "nexla_account_approval_pending",
      message: "New self-signup account requested. Needs approval",
      user_acq_type: event_data["acquisition_type"],
      env: get_env_name,
      request_id: event_data['request_id'],
      signup_request_info: {
        email: event_data['email'],
        full_name: event_data['full_name']
      }
    }

    send_data(payload, ENV['APPROVAL_EVENT_HOOK_URL'])
  end

  def send_data(payload, url)
    opts = {
      method: :post,
      url: url,
      payload: payload.to_json,
      headers: {
        content_type: :json
      }
    }
    RestClient::Request.new(opts).execute
  rescue StandardError => e
    Rails.logger.error("Error sending data to user event webhook: #{e.message}")
  end

  def get_env_name
    env = ENV['ALLOWED_ORIGINS'].split(',').first

    env.sub(/\Ahttp(s)?:(\/\/)?/, '')
  end
end
