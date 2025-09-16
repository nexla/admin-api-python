json.(team, :id)
json.partial! @api_root + "users/owner", user: team.owner
json.partial! @api_root + "orgs/brief", org: team.org
json.member current_user.team_member?(team)
json.access_roles team.get_access_roles(current_user, current_org)
json.(team, :name, :description)
json.members team.members do |m|
  json.partial! @api_root + 'teams/members/show', member: m, team: team
end
json.(team, :updated_at, :created_at)
json.tags team.tags_list
