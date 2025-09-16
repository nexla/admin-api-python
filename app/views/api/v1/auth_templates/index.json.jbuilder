if @brief
  json.array! @auth_templates do |auth_template|
    json.partial! @api_root + "auth_templates/brief", auth_template: auth_template
  end
else
  json.array! @auth_templates do |auth_template|

    json.(auth_template,
      :id, :name, :connector_id, :display_name, :description,
      :config
    )

    json.vendor do
      json.(auth_template.vendor,
        :id, :name, :display_name, :description,
        :connection_type, :auth_template, :config,
        :small_logo, :logo, :updated_at, :created_at
      )
    end

    json.connector do
      json.(auth_template.connector,
        :id, :type, :connection_type, :name,
        :description, :nexset_api_compatible,
        :updated_at, :created_at
      )
    end

    json.(auth_template, :updated_at, :created_at)

  end
end





