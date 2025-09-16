json.array! @referenced_by do |resource|
  json.type resource.class.name
  json.(resource, :id, :name, :owner_id, :org_id)
end
