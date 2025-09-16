json.domain do
  domain = approval_request.topic.domains.first
  if domain.present?
    json.partial! @api_root + "marketplace/domains/show", domain: approval_request.topic.domains.first
  else
    json.nil!
  end
end

json.data_set do
  data_set = approval_request.topic.data_set

  json.(data_set, :id, :name)
  json.owner do
    json.partial! @api_root + "orgs/member", org_member: approval_request.org.org_memberships.find_by(user_id: data_set.owner.id)
  end
end
