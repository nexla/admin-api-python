json.(approval_request, 
  :id,
  :org_id,
  :request_type,
  :topic_id,
  :status,
  :updated_at,
  :created_at
)

json.requestor do
  json.partial! @api_root + "orgs/member", org_member: approval_request.org.org_memberships.find_by(user_id: approval_request.requestor.id)
end

json.partial! @api_root + "approval_requests/extra_fields/#{approval_request.request_type}", approval_request: approval_request

if approval_request.pending?
  json.(approval_request.current_step,
    :assignee_id
  )
end

if approval_request.rejected?
  json.rejection_reason approval_request.rejection_reason
end
