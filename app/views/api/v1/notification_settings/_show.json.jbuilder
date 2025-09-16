json.(notification_setting, :id)
json.partial! @api_root + "users/owner", user: notification_setting.owner
json.partial! @api_root + "orgs/brief", org: notification_setting.org

json.(notification_setting,
    :channel,
    :notification_resource_type,
    :resource_id,
    :config,
    :priority,
    :status,
    :notification_type_id)

json.notification_type_default notification_setting.notification_type.default

if (!notification_setting.notification_channel_setting.nil?)
  json.notification_channel_setting do
    json.partial! @api_root + "notification_channel_settings/show", notification_channel_setting: notification_setting.notification_channel_setting
  end
end