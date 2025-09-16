class NotificationType < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema

  has_many :notification_channel_settings

  Event_Types = API_NOTIFICATION_EVENT_TYPES
  Resource_Types = API_NOTIFICATION_RESOURCE_TYPES

  scope :visible, -> { where(visible: true) }

  def self.event_types_enum
    enum = "ENUM("
    first = true
    Event_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.resource_types_enum
    enum = "ENUM("
    first = true
    Resource_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.load_notification_types_from_config
    return if !ActiveRecord::Base.connection.table_exists?(self.table_name)

    notification_types = JSON.parse(File.read("#{Rails.root}/config/api/notification_types.json"))
    notification_types.each do |nt|
      next if NotificationType.find_by(code: nt["code"]).present?
      NotificationType.create(nt)
    end
  end
end
