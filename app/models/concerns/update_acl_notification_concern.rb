module UpdateAclNotificationConcern
  extend ActiveSupport::Concern

  def notify_acl_changed(changed_acl_data)
    Notifications::ResourceNotifier.new(self, :update_acl, changed_acl_data).call
  end
end