json.(cluster_endpoint, :id)
json.partial! @api_root + "orgs/brief", org: cluster_endpoint.org
json.(cluster_endpoint,
    :cluster_id,
    :service,
    :protocol,
    :host,
    :port,
    :context
)
