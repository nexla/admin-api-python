json.(auth_param, :id, :name, :display_name, :description, :data_type, :order, :config, :secured, :global)

if !auth_param.vendor.nil?
  json.vendor do
    json.(auth_param.vendor, :id, :name, :display_name)
  end
else
  json.vendor({})
end

if !auth_param.auth_template.nil?
  json.auth_template do
    json.(auth_param.auth_template, :id, :name, :display_name)
  end
else
  json.auth_template({})
end

json.allowed_values auth_param.allowed_values.blank? ? [] : auth_param.allowed_values

json.(auth_param,
  :updated_at,
  :created_at)
