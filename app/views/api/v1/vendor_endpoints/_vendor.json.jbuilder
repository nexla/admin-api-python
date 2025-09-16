json.(vendor_endpoint, :id, :name, :display_name, :description, :source_template, :connection_type, :sink_template, :config)

if vendor_endpoint.resource_parameters.blank?
  json.resource_parameters []
else
  json.resource_parameters do
    json.array! vendor_endpoint.resource_parameters, partial: @api_root + 'resource_parameters/brief', as: :resource_param
  end
end

json.resource_type vendor_endpoint.resource_type

json.(vendor_endpoint,
  :updated_at,
  :created_at)