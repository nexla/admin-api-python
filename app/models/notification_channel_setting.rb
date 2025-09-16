class NotificationChannelSetting < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  has_many   :notification_settings

  json_accessor :config

  attr_accessor :control_messages_enabled

  after_initialize do
    self.control_messages_enabled = true
  end

  after_commit :handle_after_commit_create, on: :create
  after_commit :handle_after_commit_update, on: :update

  Channels = {
      :app        => 'APP',
      :email      => 'EMAIL',
      :sms        => 'SMS',
      :slack      => 'SLACK',
      :webhooks   => 'WEBHOOKS'
  }

  def self.validate_notification_channel_str (type_str)
    return nil if type_str.class != String
    return Channels.find { |sym, str| str == type_str }
  end

  def self.build_from_input (input, user, org = nil)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    input[:owner_id] = user.id
    input[:org_id] = org.nil? ? nil : org.id

    channel_info = NotificationChannelSetting.validate_notification_channel_str(input[:channel])
    raise Api::V1::ApiError.new(:bad_request, "Unknown Channel type") if channel_info.nil?

    notification_channel_setting = NotificationChannelSetting.new
    notification_channel_setting.set_defaults(user, org)
    notification_channel_setting.update_mutable!({}, user, input)

    return notification_channel_setting

  end

  def update_mutable! (request, user, input)
    return if input.nil?

    if (input.key?(:channel))
      channel_info = NotificationChannelSetting.validate_notification_channel_str(input[:channel])
      raise Api::V1::ApiError.new(:bad_request, "Unknown Channel type") if channel_info.nil?
      self.channel = channel_info[1]
    end
    self.config = input[:config] if input.key?(:config)
    self.save!
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

  def send_control_event (event_type)
    notification_settings.each do |notification_setting|
      if self.control_messages_enabled && notification_setting.control_messages_enabled
        ControlService.new(notification_setting).publish(event_type)
      end
    end
  end

  def handle_after_commit_create
    self.send_control_event(:create)
  end

  def handle_after_commit_update
    self.send_control_event(:update)
  end

end
