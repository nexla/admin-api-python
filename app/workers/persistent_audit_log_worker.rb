class PersistentAuditLogWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'default', retry: 3

  def perform(klass, controller_info, *args)
    PaperTrail.request.controller_info = controller_info
    klass = klass.constantize
    klass.perform_inline(*args) if klass.present?
  ensure
    PaperTrail.request.controller_info = nil
  end
end
