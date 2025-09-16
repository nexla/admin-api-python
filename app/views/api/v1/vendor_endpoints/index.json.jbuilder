if @brief
  json.array! @vendor_endpoints do |vendor_endpoint|
    json.partial! @api_root + "vendor_endpoints/brief", vendor_endpoint: vendor_endpoint
  end
else
  json.array! @vendor_endpoints do |vendor_endpoint|
    json.(vendor_endpoint,
      :id, :name, :display_name, :description,
      :source_template, :connection_type, :sink_template, :config
    )

    json.vendor do
      json.(vendor_endpoint.vendor,
        :id, :name, :display_name, :description,
        :connection_type, :config,
        :small_logo, :logo, :updated_at, :created_at
      )
      json.auth_templates do
        json.array! vendor_endpoint.vendor.auth_templates.ids
      end
    end

    json.resource_type vendor_endpoint.resource_type
    json.(vendor_endpoint, :updated_at, :created_at)
  end
end