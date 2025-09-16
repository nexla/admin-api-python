class NotificationSetting < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor
  include DataplaneConcern
  include ChangeTrackerConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :notification_channel_setting
  belongs_to :notification_type

  json_accessor :config

  attr_accessor :control_messages_enabled

  after_initialize do
    self.control_messages_enabled = true
  end

  after_commit :handle_after_commit_create, on: :create
  after_commit :handle_after_commit_update, on: :update
  before_destroy :handle_before_destroy

  Statuses = {
    :paused   => 'PAUSED',
    :active   => 'ACTIVE'
  }

  Resource_Types = {
    :org                => 'ORG',
    :user               => 'USER',
    :data_flow          => 'DATA_FLOW',
    :custom_data_flow   => 'CUSTOM_DATA_FLOW',
    :source             => 'SOURCE',
    :dataset            => 'DATASET',
    :sink               => 'SINK',
    :resource           => 'RESOURCE',
    :catalog_config     => 'CATALOG_CONFIG'
  }

  #uncomment to allow resource type for notification type settings endpoint
  Resource_Type_Table_Names = {
    # 'ORG' => "orgs",
    # 'USER' => "users",
    # 'DATA_FLOW' => "data_flows",
    # 'CUSTOM_DATA_FLOW' => "custom_data_flows",
    'SOURCE' => "data_sources",
    'DATASET' => "data_sets",
    'SINK' => "data_sinks"
  }

  Resource_Types_Priority = {
    'ORG'               => 1,
    'USER'              => 2,
    'FLOW'              => 2,
    'DATA_FLOW'         => 2,
    'CUSTOM_DATA_FLOW'  => 2,
    'SOURCE'            => 3,
    'DATASET'           => 3,
    'SINK'              => 3
  }

  scope :org_notifications, ->(org_id) { where(notification_resource_type: NotificationSetting::Resource_Types[:org], org_id: org_id) }

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

  def self.validate_notification_status_str (type_str)
    return nil if type_str.class != String
    return Statuses.find { |sym, str| str == type_str }
  end

  def self.validate_resource_type_str (type_str)
    return nil if type_str.class != String
    return nil if Resource_Types.find { |sym, str| str == type_str }.nil?
    type_str
  end

  def self.build_from_input (input, user, org = nil)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    input[:owner_id] = user.id
    input[:org_id] = org.nil? ? nil : org.id

    notification_setting = NotificationSetting.new

    if !(input.key?(:notification_type_id))
      raise Api::V1::ApiError.new(:bad_request, "Unknown Notification type")
    end

    channel_info = NotificationChannelSetting.validate_notification_channel_str(input[:channel])
    raise Api::V1::ApiError.new(:bad_request, "Unknown Channel type") if channel_info.nil?

    notification_setting.set_defaults(user, org)
    notification_setting.notification_type_id = input[:notification_type_id]

    if input.key?(:notification_resource_type) and input.key?(:resource_id)
      resource_type = NotificationSetting.validate_resource_type_str(input[:notification_resource_type])
      raise Api::V1::ApiError.new(:bad_request, "Unknown Resource type") if resource_type.nil?

      notification_setting.notification_resource_type = input[:notification_resource_type]
      notification_setting.resource_id = input[:resource_id]
    else
      notification_setting.notification_resource_type = 'USER'
      notification_setting.resource_id = user.id
    end
    notification_setting.priority = Resource_Types_Priority[notification_setting.notification_resource_type]

    notification_setting.update_mutable!({}, user, input)

    return notification_setting
  end

  def update_mutable! (request, user, input)
    return if input.nil?

    if (input.key?(:channel))
      channel_info = NotificationChannelSetting.validate_notification_channel_str(input[:channel])
      raise Api::V1::ApiError.new(:bad_request, "Unknown Channel type") if channel_info.nil?
      self.channel = channel_info[1]
    end

    if (input.key?(:checked) and input[:checked].truthy?)
      self.status = Statuses[:active]
    else
      self.status = Statuses[:paused]
    end

    if (input.key?(:notification_channel_setting_id))
      self.notification_channel_setting_id = input[:notification_channel_setting_id]
    end

    if (input.key?(:config))
      self.config = input[:config]
    end
    validate_uniqueness!(self.id)
    self.save!
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

  # Selects all notification settings for a given resource,
  # as well as any related USER or ORG typed notification settings
  # Params:
  # +res_type+:: resource type of queried resource
  # +res_id+:: resource ID to query
  # +user_id+:: ID of user making query
  # +org_id+:: ID of organization that the user belongs to
  def self.select_waterfall(res_type, res_id, user_id, org_id, notification_type = nil, filter_overridden_settings = false)
    settings = NotificationSetting
                 .where(notification_settings: { notification_resource_type: res_type, resource_id: res_id  })
                 .or(NotificationSetting.where(notification_resource_type: NotificationSetting::Resource_Types[:user], owner_id: user_id))

    settings = settings.or(NotificationSetting.org_notifications(org_id)) if org_id.present?

    settings = settings.where(notification_type_id: notification_type) if notification_type.present?

    settings = settings
                 .joins(:notification_type)
                 .merge(NotificationType.where(notification_types: { resource_type: [res_type, NotificationType::Resource_Types[:data_flow]] }))
                 .select('notification_settings.*,notification_types.name,notification_types.description,notification_types.code,notification_types.category,notification_types.event_type,notification_types.resource_type')

    settings = filter_waterfall(settings) if filter_overridden_settings

    settings
  end

  def self.select_notification_type(notification_type_id, owner_id, org_id=nil)

    #TODO: consider if this can be simplified to use a single query
    notification_type = NotificationType.find(notification_type_id)
    if !Resource_Type_Table_Names.has_key?(notification_type.resource_type)
      raise Api::V1::ApiError.new(:bad_request, "Unsupported Resource Type")
    end
    type = Resource_Type_Table_Names[notification_type.resource_type]

    columns = %{
      notification_settings.id as setting_id,
      notification_settings.owner_id as owner_id,
      notification_settings.org_id as org_id,
      notification_settings.channel as channel,
      notification_settings.status as status,
      notification_settings.config as setting_config,
      notification_settings.priority as priority,
      notification_settings.notification_channel_setting_id as notification_channel_setting_id,
      notification_settings.created_at as setting_created_at,
      notification_settings.updated_at as setting_updated_at,
      notification_types.id as notification_type_id,
      notification_types.name as notification_type_name,
      notification_types.description as notification_type_description,
      notification_types.code as notification_type_code,
      notification_types.category as notification_type_category,
      notification_types.event_type as notification_type_event_type,
      notification_settings.notification_resource_type,
      #{type}.id as resource_id,
      #{type}.owner_id as resource_owner_id,
      #{type}.org_id as resource_org_id,
      #{type}.name as resource_name,
      #{type}.description as resource_description,
      #{type}.status as resource_status
    }.squish

    columns_2 = %{
      notification_settings.id as setting_id,
      notification_settings.owner_id as owner_id,
      notification_settings.org_id as org_id,
      notification_settings.channel as channel,
      notification_settings.status as status,
      notification_settings.config as setting_config,
      notification_settings.priority as priority,
      notification_settings.notification_channel_setting_id as notification_channel_setting_id,
      notification_settings.created_at as setting_created_at,
      notification_settings.updated_at as setting_updated_at,
      notification_types.id as notification_type_id,
      notification_types.name as notification_type_name,
      notification_types.description as notification_type_description,
      notification_types.code as notification_type_code,
      notification_types.category as notification_type_category,
      notification_types.event_type as notification_type_event_type,
      notification_settings.notification_resource_type,
      NULL as resource_id,
      NULL as resource_owner_id,
      NULL as resource_org_id,
      NULL as resource_name,
      NULL as resource_description,
      NULL as resource_status
    }.squish

    join_string = %{
    INNER JOIN notification_types
      ON notification_settings.notification_type_id = notification_types.id
    INNER JOIN #{type}
      ON #{type}.id = notification_settings.resource_id
    WHERE notification_types.id = #{notification_type_id}
      AND notification_settings.owner_id = #{owner_id}
      AND notification_settings.notification_resource_type = '#{notification_type.resource_type}'
    }.squish

    join_string_2 = %{
    INNER JOIN notification_types
      ON notification_settings.notification_type_id = notification_types.id
    where notification_types.id = #{notification_type_id}
      AND (( notification_settings.notification_resource_type = 'USER' AND notification_settings.owner_id = #{owner_id} )
    #{org_id.nil? ? ')' : "OR ( notification_settings.notification_resource_type = 'ORG' AND notification_settings.org_id = #{org_id} ))"}
    }.squish

    NotificationSetting.joins(join_string).select(columns)
                    .union(NotificationSetting.joins(join_string_2).select(columns_2))
  end

  def self.filter_waterfall(settings)
    settings_all = settings.to_a
    keys = {}
    settings_all.each do |row|
      notification_type = row.notification_type_id
      resource_type = resource_symbol(row.notification_resource_type)
      
      if !keys.has_key? notification_type
        keys[notification_type] = resource_type
      else
        if resource_type != :org
          if keys[notification_type] == :org
            keys[notification_type] = resource_type
          else
            if keys[notification_type] == :user && resource_type == :resource
              keys[notification_type] = resource_type
            end
          end
        end
      end
    end
    filtered_settings = settings_all.select do |row|
      notification_type = row.notification_type_id
      resource_type = resource_symbol(row.notification_resource_type)
      case
      when keys[notification_type] == :org
        true
      when keys[notification_type] == :user && resource_type != :org
        true
      when keys[notification_type] == :resource && resource_type == :resource
        true
      else
        false
      end
    end
    settings.where(id: filtered_settings.map(&:id))
  end

  def self.resource_symbol(resource_type)
    case resource_type
    when "ORG"
      :org
    when "USER"
      :user
    else
      :resource
    end
  end

  def validate_uniqueness!(id)
    scope = NotificationSetting
              .where(notification_type_id: notification_type_id,
                     org_id: self.org_id,
                     resource_id: resource_id,
                     channel: channel,
                     notification_resource_type: notification_resource_type,
                     owner_id: owner_id,
                     config: read_attribute(:config))
              .where.not(id: id)

    if scope.exists?
      raise Api::V1::ApiError.new(:bad_request, "Notification setting already exists")
    end
  end

  def send_control_event (event_type)
    ControlService.new(self).publish(event_type) if self.control_messages_enabled
  end

  def handle_after_commit_create
    self.send_control_event(:create)
  end

  def handle_after_commit_update
    self.send_control_event(:update)
  end

  def handle_before_destroy
    self.send_control_event(:delete)
  end
end
