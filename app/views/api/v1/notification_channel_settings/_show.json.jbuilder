json.(notification_channel_setting, :id)
json.partial! @api_root + "users/owner", user: notification_channel_setting.owner
json.partial! @api_root + "orgs/brief", org: notification_channel_setting.org

json.(notification_channel_setting,
    :channel,
    :config)