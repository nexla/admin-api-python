json.(vendor_endpoint, :id, :name, :display_name, :description, :source_template, :connection_type, :sink_template, :config)

json.vendor do
  json.partial! @api_root + "vendors/show", vendor: vendor_endpoint.vendor
end

json.resource_type vendor_endpoint.resource_type

json.(vendor_endpoint,
    :updated_at,
    :created_at)