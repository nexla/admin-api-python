json.id org_member.user.id
json.full_name org_member.user.full_name
json.email org_member.user.email
json.is_admin?( @org_access_roles[org_member.user.id] == :admin || @org_access_roles[org_member.user.id] == :owner )
json.access_role @org_access_roles[org_member.user_id]
json.access_role_expires_at @org_roles_expirations && @org_roles_expirations[org_member.user_id]
json.org_membership_status org_member.status
json.user_status org_member.user.status
