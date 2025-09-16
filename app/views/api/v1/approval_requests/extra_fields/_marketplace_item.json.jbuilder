json.domain do
  domain = approval_request.topic
  if domain.present?
    json.partial! @api_root + "marketplace/domains/show", domain: domain
  else
    json.nil!
  end
end

fill_basic_step = approval_request.first_step
data_set_id = fill_basic_step&.result&.dig(:data_set_id)

if data_set_id.present?
  data_set = DataSet.find_by(id: data_set_id)

  json.data_set do
    json.(data_set, :id, :name)
    json.owner do
      json.partial! @api_root + "orgs/member", org_member: approval_request.org.org_memberships.find_by(user_id: data_set.owner.id)
    end
  end
end
