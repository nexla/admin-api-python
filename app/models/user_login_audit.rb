class UserLoginAudit < ApplicationRecord
  after_create :limit_user_login_audits

  def self.max_user_login_audits
    ENV.fetch('API_MAX_USER_LOGIN_AUDITS', -1).to_i
  end

  def self.prune_enabled?
    ENV.fetch('API_MAX_USER_LOGIN_AUDITS', -1).to_i > 0
  end

  belongs_to :user

  private 

  def limit_user_login_audits

    return unless UserLoginAudit.prune_enabled?

    begin
      LoginEventsWorker.perform_async(self.user_id)
    rescue => e
      logger = Rails.configuration.x.error_logger
      logger.error({
        event: "user_login_audits",
        class: "UserLoginAudit",
        id: self.id,
        error: e.message
      }.to_json)
    end
  end
  
end