class NotificationsArchive < ApplicationRecord
  self.table_name = "notifications_archive"

  def self.has_archive?
    ActiveRecord::Base.connection.table_exists?(self.table_name)
  end
end