json.partial! @api_root + "clusters/show", cluster: cluster
json.endpoints cluster.cluster_endpoints, :service, :protocol, :host, :port, :context, :id, :header_host