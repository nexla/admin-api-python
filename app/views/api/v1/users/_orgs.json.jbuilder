if !org_memberships.nil?
  json.array! org_memberships do |om|
    if om.org.nil?
      json.org_id om.org_id
      json.org nil
    else
      user = om.user
      user.org = org = om.org
      role = user_org_role(user, org)

      json.id org.id
      json.name org.name
      json.is_admin? org.has_admin_access?(user)
      json.access_role role
      json.access_role_expires_at org.role_expires_at(user, org, role)
      json.org_membership_status om.status
    end
  end
else
  json.array! []
end