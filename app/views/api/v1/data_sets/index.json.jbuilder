if @shared
  json.array! @data_sets, partial: @api_root + 'data_sets/show_shared', as: :data_set
elsif @expand
  json.array! @data_sets, partial: @api_root + 'data_sets/show', as: :data_set
else
  json.array! @data_sets do |data_set|
    # NEX-5568 it is possible to orphan a detected data_set
    # by deleting its data_source before the data set is committed
    # to the db. This can happen if there is a delay in the
    # infrastructure pause/delete while the ingestor is detecting
    # a new data set. We omit such data sets from the response.
    next if (data_set.data_source_id.present? && !data_set.data_source.present?)

    json.(data_set, :id, :origin_node_id, :flow_node_id)
    json.owner do
      json.(data_set.owner, :id, :full_name, :email)
    end
    if data_set.org.nil?
      json.org nil
    else
      json.org do
        json.(data_set.org, :id, :name, :email_domain, :client_identifier)
      end
    end
    json.(data_set,
      :name,
      :description,
      :status,
      :data_credentials_id,
      :runtime_status,
      :public,
      :managed,
      :data_source_id
    )

    json.node_type data_set.node_type if data_set.node_type != :data_set
    if (@include_nexset_api_config)
      json.nexset_api_config data_set.get_nexset_api_config
      json.sync_api_config data_set.get_nexset_api_config
    end

    if (!data_set.data_source_id.nil?)
      json.data_source do
        json.(data_set.data_source, :id, :owner_id, :org_id, :name, :description, :status, :runtime_status)
        json.source_type data_set.data_source.raw_source_type(current_user)
        json.connector do
          json.(data_set.data_source.connector,
            :id,
            :type,
            :connection_type,
            :name,
            :description,
            :nexset_api_compatible
          )
        end
        if (!data_set.data_source.vendor_endpoint_id.nil? && !data_set.data_source.vendor_endpoint.vendor_id.nil?)
          json.vendor_id data_set.data_source.vendor_endpoint.vendor.id
        else
          json.vendor_id nil
        end
      end
    end

    if (@include_summary)
      json.summary data_set.summary
    end

    json.parent_data_sets [data_set.parent_data_set].compact do |pds|
      json.(pds, :id, :owner_id, :org_id, :name, :description)
    end
    json.data_sinks data_set.data_sinks do |ds|
      json.(ds, :id, :owner_id, :org_id, :name, :description)
    end

    if @access_roles[:data_sets].present?
      json.access_roles [@access_roles[:data_sets][data_set.id]]
    else
      json.access_roles data_set.get_access_roles(current_user, current_org)
    end

    json.transform_id data_set.code_container_id
    json.output_schema data_set.output_schema_with_annotations
    json.source_config data_set.parent_source&.source_config
    json.connector_type data_set.parent_source&.connector_type
    json.partial! @api_root + "connectors/show", connector: data_set.connector
    json.(data_set, :copied_from_id, :created_at, :updated_at)

    if @tags[:data_sets].present?
      json.tags @tags[:data_sets][data_set.id]
    else
      json.tags data_set.tags_list
    end

    json.flow_type data_set.flow_type

    if data_set.org&.marketplace_enabled?
      json.domains data_set.domains do |domain|
        json.(domain, :id, :name)
      end
    end
  end
end
