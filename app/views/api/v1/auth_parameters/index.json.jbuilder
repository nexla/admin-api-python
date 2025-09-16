if (@brief)
  json.array! @auth_parameters do |auth_param|
    json.partial! @api_root + "auth_parameters/brief", auth_param: auth_param
  end
else
  json.array! @auth_parameters do |auth_param|

    json.(auth_param,
      :id, :name, :display_name, :description,
      :data_type, :order, :config, :secured, :global,
      :auth_template_id
    )

    if (auth_param.vendor.nil?)
      json.vendor nil
    else
      json.vendor do
        json.(auth_param.vendor,
          :id, :name, :display_name, :description,
          :connection_type, :auth_template, :config,
          :small_logo, :logo, :updated_at, :created_at
        )
        json.auth_templates do
          json.array! auth_param.vendor.auth_templates.ids
        end
      end
    end

    if (auth_param.auth_template.nil?)
      json.auth_template nil
    else
      json.auth_template do
        json.(auth_param.auth_template,
          :id, :name, :display_name, :description,
          :config, :vendor_id, :updated_at, :created_at
        )
      end
    end

    json.allowed_values auth_param.allowed_values.blank? ? [] : auth_param.allowed_values
    json.(auth_param, :updated_at, :created_at)

  end
end




