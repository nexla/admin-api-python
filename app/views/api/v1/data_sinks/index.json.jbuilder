if @expand
  json.array! @data_sinks, partial: @api_root + 'data_sinks/show', as: :data_sink
else
  json.array! @data_sinks do |data_sink|

    json.(data_sink,
      :id,
      :origin_node_id,
      :flow_node_id,
      :name,
      :description,
      :status,
      :runtime_status,
      :data_set_id,
      :data_map_id,
      :data_source_id,
      :sink_format,
      :sink_schedule,
      :in_memory,
      :managed,
      :ingestion_mode,
      :copied_from_id,
      :updated_at,
      :created_at
    )

    json.sink_type data_sink.raw_sink_type(current_user)
    json.connector_type data_sink.connector_type
    json.partial! @api_root + "connectors/show", connector: data_sink.connector

    if @access_roles[:data_sinks].present?
      json.access_roles [@access_roles[:data_sinks][data_sink.id]]
    else
      json.access_roles data_sink.get_access_roles(current_user, current_org)
    end

    json.owner do
      json.(data_sink.owner, :id, :full_name, :email)
    end

    if data_sink.org.nil?
      json.org nil
    else
      json.org do
        json.(data_sink.org, :id, :name, :email_domain, :client_identifier)
      end
    end

    if data_sink.data_set.nil?
      json.data_set nil
    else
      json.data_set do
        json.(data_sink.data_set,
          :id,
          :owner_id,
          :org_id,
          :name,
          :description,
          :status,
          :copied_from_id,
          :created_at,
          :updated_at
        )
      end
    end

    if data_sink.data_map.nil?
      json.data_map nil
    else
      json.data_map do
        json.(data_sink.data_map,
          :id,
          :owner_id,
          :org_id,
          :name,
          :description,
          :public,
          :created_at,
          :updated_at
        )
      end
    end

    if data_sink.has_template?
      json.has_template true
      json.vendor_endpoint do
        json.(data_sink.vendor_endpoint, :id, :name, :display_name)
      end
      if (!data_sink.vendor_endpoint.vendor.nil?)
        json.vendor do
          json.(data_sink.vendor_endpoint.vendor, :id, :name, :display_name, :connection_type)
        end
      else
        json.vendor nil
      end
    end

    if @tags[:data_sinks].present?
      json.tags @tags[:data_sinks][data_sink.id]
    else
      json.tags data_sink.tags_list
    end

    json.flow_type data_sink.flow_type
  end
end
