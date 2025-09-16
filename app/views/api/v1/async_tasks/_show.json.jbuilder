json.(async_task, :id, :task_type, :status, :progress, :priority, :created_at, :started_at, :stopped_at, :error, :arguments, :result, :result_url)

if current_org.has_admin_access?(current_user)
  json.partial! @api_root + "users/owner", user: async_task.owner
end

if current_user.super_user?
  json.partial! @api_root + "orgs/brief", org: async_task.org
end