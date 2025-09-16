json.(user_setting, :id)
json.partial! @api_root + "users/owner", user: user_setting.owner
json.partial! @api_root + "orgs/brief", org: user_setting.org
json.user_settings_type user_setting.user_settings_type.name
json.(user_setting,
  :primary_key_value,
  :description,
  :settings,
  :copied_from_id,
  :updated_at,
  :created_at
)
