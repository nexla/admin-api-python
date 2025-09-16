json.(@token, :access_token, :token_type, :expires_in)
json.user do
  json.partial! @api_root + "users/show", user: current_user
end
if (current_org.nil?)
  json.org nil
else
  if (!@api_org_membership.nil?)
    json.org_membership do
      json.(@api_org_membership, :api_key, :status)
      json.is_admin? @api_org_membership.org.has_admin_access?(current_user)
      role = user_org_role(current_user, current_org)
      json.access_role role
      json.access_role_expires_at @api_org_membership.org.role_expires_at(current_user, @api_org_membership.org, role)
    end
  end
  json.partial! @api_root + "orgs/brief", org: current_org, as: :org, show_webhook_host: true
end