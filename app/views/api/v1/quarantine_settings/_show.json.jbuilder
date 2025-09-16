json.(quarantine_setting, :id)
json.partial! @api_root + "users/owner", user: quarantine_setting.owner
json.partial! @api_root + "orgs/brief", org: quarantine_setting.org

json.(quarantine_setting,
    :resource_type,
    :resource_id,
    :config,
    :data_credentials_id)

if (!quarantine_setting.data_credentials.nil?)
  json.credentials_type quarantine_setting.data_credentials.credentials_type
  json.data_credentials do
    json.partial! @api_root + "data_credentials/show", data_credentials: quarantine_setting.data_credentials
  end
else
  json.data_credentials nil
end