json.array! @clusters do |cluster|
  json.partial! @api_root + "clusters/show_with_endpoints", cluster: cluster
end