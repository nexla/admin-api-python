json.(data_source, :id, :origin_node_id, :flow_node_id)

json.owner do
  json.(data_source.owner, :id, :full_name, :email, :email_verified_at)
end
json.org do
  if data_source.org_id.nil?
    json.nil!
  else
    json.(data_source.org,
      :id,
      :name,
      :cluster_id,
      :new_cluster_id,
      :cluster_status,
      :status,
      :email_domain,
      :email,
      :client_identifier
    )
  end
end

if @access_roles[:data_sources].present?
  json.access_roles [@access_roles[:data_sources][data_source.id]]
else
  json.access_roles data_source.get_access_roles(current_user, current_org)
end

json.(data_source,
  :name,
  :description,
  :status,
  :runtime_status,
  :copied_from_id,
  :updated_at,
  :created_at)

json.source_type data_source.raw_source_type(current_user)
json.connector_type data_source.connector_type
json.connector do
  json.(data_source.connector,
    :id,
    :type,
    :connection_type,
    :name,
    :description,
    :nexset_api_compatible
  )
end

if data_source.has_template?
  json.vendor_endpoint do
    json.partial! @api_root + "vendor_endpoints/brief", vendor_endpoint: data_source.vendor_endpoint
  end
  if (!data_source.vendor_endpoint.nil? && !data_source.vendor_endpoint.vendor.nil?)
    json.vendor do
      json.partial! @api_root + "vendors/brief", vendor: data_source.vendor_endpoint.vendor
    end
  else
    json.vendor nil
  end
end
