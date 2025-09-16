if @expand
  @access_roles[:code_containers] = @access_roles.delete(:transforms)
  @tags[:code_containers] = @tags.delete(:transforms)
  json.array! @transforms, partial: @api_root + 'code_containers/show', as: :code_container
else
  json.array! @transforms do |code_container|

    json.(code_container,
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
      json.(code_container.owner, :id, :full_name, :email)
    end

    if code_container.org.nil?
      json.org nil
    else
      json.org do
        json.(code_container.org, :id, :name, :email_domain, :client_identifier)
      end
    end

    if @access_roles[:transforms].present?
      json.access_roles [@access_roles[:transforms][code_container.id]]
    else
      json.access_roles code_container.get_access_roles(current_user, current_org)
    end

    json.data_sets code_container.data_sets.map(&:id)

    if @tags[:transforms].present?
      json.tags @tags[:transforms][code_container.id]
    else
      json.tags code_container.tags_list
    end
  end
end

