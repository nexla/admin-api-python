if @expand
  json.array! @code_containers, partial: @api_root + 'code_containers/show', as: :code_container
else
  json.array! @code_containers do |code_container|

    json.(code_container,
      :id,
      :name,
      :description,
      :resource_type,
      :ai_function_type,
      :data_credentials_id,
      :runtime_data_credentials_id,
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
      json.(code_container.owner, :id, :full_name, :email)
    end

    if code_container.org.nil?
      json.org nil
    else
      json.org do
        json.(code_container.org, :id, :name, :email_domain, :client_identifier)
      end
    end

    if @access_roles[:code_containers].present?
      json.access_roles [@access_roles[:code_containers][code_container.id]]
    else
      json.access_roles code_container.get_access_roles(current_user, current_org)
    end

    json.data_sets code_container.data_sets.map(&:id)

    if @tags[:code_containers].present?
      json.tags @tags[:code_containers][code_container.id]
    else
      json.tags code_container.tags_list
    end

  end
end
