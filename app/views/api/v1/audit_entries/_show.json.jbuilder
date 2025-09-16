json.(audit_entry, :id, :item_type, :item_id, :event, :org_id)
json.(audit_entry, :impersonator_id, :owner_id, :owner_email, :request_ip, :request_user_agent, :request_url)

json.user do
  json.id audit_entry.user_id
  json.email audit_entry.user_email
end

if (@expand)
  if (!audit_entry.impersonator_id.nil? and !audit_entry.impersonator.nil?)
    json.impersonator do
      json.partial! @api_root + "users/impersonator", user: audit_entry.impersonator
    end
  end

  if (!audit_entry.owner_id.nil? and !audit_entry.owner.nil?)
    json.partial! @api_root + "users/owner", user: audit_entry.owner
  end
end

json.(audit_entry, :resource_type, :resource_id) if audit_entry.respond_to?(:resource_type)
json.(audit_entry, :change_summary, :association_resource)

if audit_entry.object_changes.blank?
  json.object_changes({})
else
  json.object_changes (audit_entry.wrap_object_changes)
end

json.(audit_entry, :created_at)
