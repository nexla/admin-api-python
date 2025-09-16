json.(notification, :id)
json.partial! @api_root + "users/owner", user: notification.owner
json.partial! @api_root + "orgs/brief", org: notification.org
json.access_roles notification.get_access_roles(current_user, current_org)
json.(notification, :level, :resource_id, :resource_type, :message_id, :message, :read_at, :updated_at, :created_at)
json.ts notification.timestamp&.to_i