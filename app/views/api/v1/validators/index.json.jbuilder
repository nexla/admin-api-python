if @expand
  @access_roles[:code_containers] = @access_roles.delete(:validators)
  @tags[:code_containers] = @tags.delete(:validator)
  json.array! @validators, partial: @api_root + 'code_containers/show', as: :code_container
else
  json.array! @validators do |validator|

    json.(validator,
      :id,
      :name,
      :description,
      :resource_type,
      :reusable,
      :public,
      :managed,
      :code_type,
      :output_type,
      :code_config,
      :code_encoding,
      :code,
      :custom_config,
      :copied_from_id,
      :updated_at,
      :created_at
    )

    json.owner do
      json.(validator.owner, :id, :full_name, :email)
    end

    if validator.org.nil?
      json.org nil
    else
      json.org do
        json.(validator.org, :id, :name, :email_domain, :client_identifier)
      end
    end

    if @access_roles[:validators].present?
      json.access_roles [@access_roles[:validators][validator.id]]
    else
      json.access_roles validator.get_access_roles(current_user, current_org)
    end

    json.data_sets validator.data_sets.map(&:id)

    if @tags[:validators].present?
      json.tags @tags[:validators][validator.id]
    else
      json.tags validator.tags_list
    end

  end
end
