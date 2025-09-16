json.array! @org.clusters do |cluster|
  json.partial! @api_root + "clusters/show", cluster: cluster
end