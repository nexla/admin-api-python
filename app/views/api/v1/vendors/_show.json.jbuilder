json.(vendor, 
  :id, 
  :name, 
  :display_name, 
  :description, 
  :config, 
  :small_logo, 
  :logo)
json.connection_type vendor.connector.type if !vendor.connector.nil?
json.auth_templates do
  if @expand
    json.array! vendor.auth_templates do |auth_template|
      json.partial!  @api_root + 'auth_templates/show', auth_template: auth_template
    end
  else
    json.array! vendor.auth_templates.ids
  end
end

json.vendor_endpoints do
  if @expand
    json.array! vendor.vendor_endpoints do |vendor_endpoint|
      json.partial!  @api_root + 'vendor_endpoints/vendor', vendor_endpoint: vendor_endpoint
    end
  else
    json.array! vendor.vendor_endpoints.ids
  end
end

json.(vendor,
  :updated_at,
  :created_at
)