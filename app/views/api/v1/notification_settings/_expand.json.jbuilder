json.(notification_setting, :id)

json.(notification_setting,
  :org_id,
  :owner_id,
  :channel,
  :notification_resource_type,
  :resource_id,
  :config,
  :priority,
  :status,
  :notification_type_id,
  :name,
  :description,
  :code,
  :category,
  :event_type)

json.resource_type notification_setting.notification_resource_type