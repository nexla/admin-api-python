json.status @async_task.wrapped_status
json.request_id @async_task.id
json.request_started_at @async_task.started_at
if @async_task.running?
  json.progress @async_task.progress
elsif @async_task.completed?
  json.request_completed_at @async_task.stopped_at
  json.response @async_task.result
elsif @async_task.stopped_at.present?
  json.request_stopped_at @async_task.stopped_at
end