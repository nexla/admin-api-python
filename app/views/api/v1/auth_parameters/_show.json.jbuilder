json.(auth_param, :id, :name, :display_name, :description, :data_type, :order, :config, :secured, :global)

if !(auth_param.vendor.nil?)
  json.vendor do
    json.partial! @api_root + "vendors/show", vendor: auth_param.vendor
  end
else
  json.vendor nil
end

if !(auth_param.auth_template.nil?)
  json.auth_template do
    json.partial! @api_root + "auth_templates/brief", auth_template: auth_param.auth_template
  end
else
  json.auth_template nil
end

json.allowed_values auth_param.allowed_values.blank? ? [] : auth_param.allowed_values

json.(auth_param,
    :updated_at,
    :created_at)