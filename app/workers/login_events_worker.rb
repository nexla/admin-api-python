class LoginEventsWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'login_events'

  def perform(user_id)
    return unless UserLoginAudit.prune_enabled?

    begin
      retries ||= 0
      max_login_events = UserLoginAudit.max_user_login_audits

      user = User.find(user_id)
      return if user.user_login_audits.count <= max_login_events
      user.user_login_audits.where(id: user.user_login_audits.offset(max_login_events).ids).delete_all
    rescue
      retry if (retries += 1) < 3
    end
  end
end
  