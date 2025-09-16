path = 'notification_settings/show'
if (@show_brief)
  path = 'notification_settings/brief'
end
json.array! @notification_setting, partial: @api_root + path, as: :notification_setting