json.(@token, :access_token, :token_type, :expires_in)
json.user do
  json.partial! @api_root + "users/show", user: current_user, show_api_key: true
end
if (current_org.nil?)
  json.org nil
else
  if (!@api_org_membership.nil?)
    json.org_membership do
      json.(@api_org_membership, :api_key, :status)
    end
  end
  json.org do
    json.partial! @api_root + "orgs/show", org: current_org
  end
end