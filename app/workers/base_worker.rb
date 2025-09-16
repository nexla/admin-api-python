class BaseWorker
  include Sidekiq::Worker

  def self.perform_async_with_audit_log(*args)
    PersistentAuditLogWorker.perform_async(
      self.name, PaperTrail.request.controller_info.stringify_keys, *args)
  end
end
