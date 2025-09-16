json.(auth_param, :id, :name, :display_name, :description, :data_type, :order, :config, :secured, :global)
json.allowed_values auth_param.allowed_values.blank? ? [] : auth_param.allowed_values