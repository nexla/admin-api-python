if @expand
  json.array! @data_sources, partial: @api_root + 'data_sources/show', as: :data_source
else
  json.array! @data_sources do |data_source|

    json.(data_source,
      :id, :origin_node_id, :flow_node_id,
      :name, :description, :status, :runtime_status,
      :ingest_method, :source_format,
      :managed, :code_container_id,
      :copied_from_id, :created_at, :updated_at
    )
    # Once UI stops relying on source_type field. We can remove in favor of connector object field.
    json.source_type data_source.raw_source_type(current_user)
    json.connector_type data_source.connector_type
    json.partial! @api_root + "connectors/show", connector: data_source.connector

    if @access_roles[:data_sources].present?
      json.access_roles [@access_roles[:data_sources][data_source.id]]
    else
      json.access_roles data_source.get_access_roles(current_user, current_org)
    end

    json.auto_generated data_source.auto_generated

    json.owner do
      json.(data_source.owner, :id, :full_name, :email)
    end

    if data_source.org.nil?
      json.org nil
    else
      json.org do
        json.(data_source.org, :id, :name, :email_domain, :client_identifier,
          :cluster_id, :new_cluster_id, :cluster_status)
      end
    end

    json.data_sets(data_source.data_sets.map { |ds| 
      {
        :id => ds.id,
        :owner_id => ds.owner_id,
        :org_id => ds.org_id,
        :name => ds.name,
        :description => ds.description,
        :created_at => ds.created_at,
        :updated_at => ds.updated_at
      } if !ds.nil?
    })

    if data_source.has_template?
      json.has_template true
      json.vendor_endpoint do
        json.(data_source.vendor_endpoint, :id, :name, :display_name)
      end
      if (!data_source.vendor_endpoint.vendor.nil?)
        json.vendor do
          json.(data_source.vendor_endpoint.vendor, :id, :name, :display_name, :connection_type)
        end
      else
        json.vendor nil
      end
    end

    if @tags[:data_sources].present?
      json.tags @tags[:data_sources][data_source.id]
    else
      json.tags data_source.tags_list
    end

    runs = @run_ids.present? ? @run_ids[data_source.id].first(5) :
      data_source.runs

    json.run_ids(runs) do |run|
      json.id run.run_id
      json.created_at run.created_at
    end

    json.flow_type data_source.flow_type
    json.ingestion_mode data_source.ingestion_mode
  end
end
