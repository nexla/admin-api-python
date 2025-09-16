json.(auth_template, :name, :display_name, :description, :config, :id)
json.credentials_type auth_template.connector.type

if !auth_template.vendor.nil?
  json.vendor do
    json.(auth_template.vendor, :id, :name, :display_name)
  end
else
  json.vendor({})
end

if !auth_template.auth_parameters.blank?
  json.auth_parameters do
    json.array! auth_template.auth_parameters, partial: @api_root + 'auth_parameters/show_inlined', as: :auth_param
  end
else
  json.auth_parameters []
end
