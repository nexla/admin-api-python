if @expand
  json.array! @data_maps, partial: @api_root + 'data_maps/show', as: :data_map
else
  json.array! @data_maps do |data_map|

    json.(data_map,
      :id,
      :name,
      :description,
      :public,
      :managed,
      :data_type,
      :data_format,
      :data_sink_id,
      :emit_data_default,
      :use_versioning,
      :map_primary_key,
      :data_defaults,
      :updated_at,
      :created_at
    )

    json.owner do
      json.(data_map.owner, :id, :full_name, :email)
    end

    if data_map.org.nil?
      json.org nil
    else
      json.org do
        json.(data_map.org, :id, :name, :email_domain, :client_identifier)
      end
    end
 
    if @access_roles[:data_maps].present?
      json.access_roles [@access_roles[:data_maps][data_map.id]]
    else
      json.access_roles data_map.get_access_roles(current_user, current_org)
    end

    json.data_set_id data_map.data_set&.id
    json.map_entry_count data_map.get_map_entry_count(false)
    json.map_entry_schema data_map.get_map_entry_schema

    if @tags[:data_maps].present?
      json.tags @tags[:data_maps][data_map.id]
    else
      json.tags data_map.tags_list
    end

  end
end
