class TransferUserResourcesWorker < BaseWorker
  include Sidekiq::Worker
  
  sidekiq_options queue: 'transfer_user_resources'

  def perform user_id, org_id, delegate_owner_id
    user = User.find_by(id: user_id)
    delegate_owner = User.find_by(id: delegate_owner_id)
    org = org_id.present? ? Org.find_by(id: org_id) : nil
    begin
      retries ||= 0
      TransferUserResources.transfer(user, org, delegate_owner)
    rescue => e
      logger = Rails.configuration.x.transfer_user_resource_logger
      logger.error("EXCEPTION: #{e.message}, #{e.backtrace[0..5]}")
      retry if (retries += 1) < 3
    end
  end
  
end
    