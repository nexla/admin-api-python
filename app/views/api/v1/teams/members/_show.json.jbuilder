json.(member, :id, :email)
member.org = team.org
json.admin team.has_admin_access?(member)