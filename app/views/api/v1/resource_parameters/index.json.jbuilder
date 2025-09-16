if @brief
  json.array! @resource_parameters do |resource_parameter|
    json.partial! @api_root + "resource_parameters/brief", resource_param: resource_parameter
  end
else
  json.array! @resource_parameters do |resource_param|

    json.(resource_param,
      :id, :name, :display_name, :description,
      :resource_type, :data_type, :order, :config, :global
    )

    if (resource_param.vendor_endpoint.nil?)
      json.vendor_endpoint nil
    else
      json.vendor_endpoint do
        json.(resource_param.vendor_endpoint,
          :id, :name, :display_name, :description, :source_template,
          :connection_type, :sink_template, :config
        )
        json.vendor do
          json.(resource_param.vendor_endpoint.vendor,
            :id, :name, :display_name, :description,
            :connection_type, :auth_template, :config,
            :small_logo, :logo, :updated_at, :created_at
          )
        end

        json.resource_type resource_param.vendor_endpoint.resource_type
        json.(resource_param.vendor_endpoint, :updated_at, :created_at)
      end
    end

    json.allowed_values resource_param.allowed_values.blank? ? [] : resource_param.allowed_values
    json.(resource_param, :updated_at, :created_at)
  end
end