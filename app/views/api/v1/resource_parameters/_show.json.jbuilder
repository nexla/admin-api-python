json.(resource_param, :id, :name, :display_name, :description, :resource_type, :data_type, :order, :config, :global)

if !(resource_param.vendor_endpoint.nil?)
  json.vendor_endpoint do
    json.partial! @api_root + "vendor_endpoints/show", vendor_endpoint: resource_param.vendor_endpoint
  end
else
  json.vendor_endpoint nil
end

json.allowed_values resource_param.allowed_values.blank? ? [] : resource_param.allowed_values

json.(resource_param,
    :updated_at,
    :created_at)