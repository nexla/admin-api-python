json.array! custodians do |custodian|
  json.(custodian, :id, :email, :full_name)

  custodian.org = current_org # Set org for context of access_roles
  json.access_roles current_org.get_access_roles(custodian, current_org)
end
