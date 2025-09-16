json.(auth_template, :id, :name, :display_name, :description, :config)
json.credentials_type auth_template.connector.type

json.auth_parameters do
  json.array! auth_template.auth_parameters.ids
end

if !auth_template.vendor.nil?
  json.vendor do
    json.(auth_template.vendor, :id, :name, :display_name)
  end
else
  json.vendor({})
end
