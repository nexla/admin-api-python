if @expand
  json.array! @data_schemas, partial: @api_root + 'data_schemas/show', as: :data_schema
else
  json.array! @data_schemas do |data_schema|

    json.(data_schema,
      :id,
      :name,
      :description,
      :detected,
      :managed,
      :template,
      :public,
      :schema,
      :annotations,
      :validations,
      :data_samples,
      :updated_at,
      :created_at
    )

    json.owner do
      json.(data_schema.owner, :id, :full_name, :email)
    end

    if data_schema.org.nil?
      json.org nil
    else
      json.org do
        json.(data_schema.org, :id, :name, :email_domain, :client_identifier)
      end
    end

    if @access_roles[:data_schemas].present?
      json.access_roles [@access_roles[:data_schemas][data_schema.id]]
    else
      json.access_roles data_schema.get_access_roles(current_user, current_org)
    end

    json.data_sets data_schema.data_sets.map(&:id)
    json.version data_schema.latest_version

    if @tags[:data_schemas].present?
      json.tags @tags[:data_schemas][data_schema.id]
    else
      json.tags data_schema.tags_list
    end

  end
end

