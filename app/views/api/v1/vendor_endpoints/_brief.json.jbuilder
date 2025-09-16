json.(vendor_endpoint, :id, :name, :display_name)

if !vendor_endpoint.vendor.nil?
  json.vendor do
    json.(vendor_endpoint.vendor, :id, :name, :display_name)
  end
else
  json.vendor({})
end

json.resource_type vendor_endpoint.resource_type

json.(vendor_endpoint,
  :updated_at,
  :created_at)
