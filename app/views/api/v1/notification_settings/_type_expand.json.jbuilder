json.(notification_setting, :setting_id)

json.(notification_setting,
  :org_id,
  :owner_id,
  :channel)

json.resource_type notification_setting.notification_resource_type

json.(notification_setting,
  :resource_id,
  :setting_config,
  :priority,
  :status,
  :notification_type_id,
  :setting_created_at,
  :setting_updated_at,
  :notification_type_name,
  :notification_type_description,
  :notification_type_code,
  :notification_type_category,
  :notification_type_event_type,
  :resource_owner_id,
  :resource_org_id,
  :resource_name,
  :resource_description,
  :resource_status)