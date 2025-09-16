
flow_node.resources.each do |res, res_list|

  json.set! res, res_list do |r|
    json.(r, :id, :owner_id, :org_id)
  end

end