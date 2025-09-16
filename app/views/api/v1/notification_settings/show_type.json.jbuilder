if (@expand)
  path = 'notification_settings/type_expand'
else
  path = 'notification_settings/type_brief'
end

json.array! @notification_setting, partial: @api_root + path, as: :notification_setting
