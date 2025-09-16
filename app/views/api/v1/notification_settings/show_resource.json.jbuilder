if (@expand)
  path = 'notification_settings/expand'
else
  path = 'notification_settings/brief'
end

  json.array! @notification_setting, partial: @api_root + path, as: :notification_setting