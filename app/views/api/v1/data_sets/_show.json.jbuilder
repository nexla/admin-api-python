if @shared
  json.partial! @api_root + "data_sets/show_shared", data_set: data_set
else
  json.(data_set, :id, :origin_node_id, :flow_node_id)
  json.(data_set.flow_node, :parent_node_id)

  json.owner do
    json.(data_set.owner, :id, :full_name, :email, :email_verified_at)
  end
  json.org do
    if data_set.org_id.nil?
      json.nil!
    else
      json.(data_set.org,
        :id,
        :name,
        :cluster_id,
        :new_cluster_id,
        :cluster_status,
        :status,
        :email_domain,
        :email,
        :client_identifier
      )
    end
  end
  json.version data_set.get_latest_version
  json.(data_set, :name, :description)

  if @access_roles[:data_sets].present?
    access_role = @access_roles[:data_sets][data_set.id]
    json.access_roles [access_role]
    has_collaborator_access = (access_role != :sbarer)
  else
    json.access_roles data_set.get_access_roles(current_user, current_org)
    has_collaborator_access = data_set.has_collaborator_access?(current_user)
  end

  json.(data_set, :status, :data_credentials_id, :sample_service_id, :source_path, :public, :managed)

  json.(data_set, :data_source_id)
  if (@expand && !data_set.data_source_id.nil?)
    json.data_source do
      json.partial! @api_root + "data_sources/brief", data_source: data_set.data_source
    end
  end

  json.node_type data_set.node_type if data_set.node_type != :data_set
  json.nexset_api_config data_set.get_nexset_api_config
  json.sync_api_config data_set.get_nexset_api_config

  json.api_keys(data_set.api_keys) do |k|
    json.(k,
      :id,
      :owner_id,
      :org_id,
      :data_set_id,
      :name,
      :description,
      :status,
      :scope,
      :api_key,
      :url,
      :last_rotated_key,
      :last_rotated_at,
      :updated_at,
      :created_at
    )
  end

  if (!data_set.org.nil?)
    json.catalog_configs(data_set.org.catalog_configs) do |cc|
      json.(cc, :id, :name, :status, :config, :data_credentials_id)
    end
  end

  json.catalog_refs(data_set.catalog_refs) do |cr|
    json.(cr,
      :id,
      :catalog_config_id,
      :status,
      :reference_id,
      :link,
      :error_msg,
      :updated_at,
      :created_at
    )
  end

  if (@expand)
    json.(data_set, :source_schema_id, :source_schema)
  end

  json.parent_data_set_ids [data_set.parent_data_set_id].compact

  json.parent_data_sets [data_set.parent_data_set].compact do |ds|
    # Note, for backwards compatibility we return parent
    # data set in an array called parent_data_sets.
    json.(ds, :id, :owner_id, :org_id, :name, :description, :updated_at, :created_at)
    if (@expand)
      if @access_roles[:data_sets].present?
        parent_access_role = @access_roles[:data_sets][ds.id]
        can_manage = [:owner, :admin].include?(parent_access_role)
      else
        can_manage = can?(:manage, ds)
      end
      if can_manage
        json.(ds, :source_schema)
        if (ds.respond_to?(:code_container_id))
          json.transform_id ds.code_container_id
        end
        json.(ds, :transform, :data_source_id)
        if (!ds.data_source_id.nil? && !ds.data_source.nil?)
          json.data_source(ds.data_source, :id, :owner_id, :org_id)
        end
      end

      if has_collaborator_access
        json.output_schema ds.output_schema_with_annotations
        ds_source = ds.parent_source
        json.source_config ds_source&.source_config
        json.connector_type ds_source&.connector_type
        json.partial! @api_root + "connectors/show", connector: ds.connector
      end
    end
  end

  json.data_sinks data_set.data_sinks do |sink|
    if (@expand && has_collaborator_access)
      json.partial! @api_root + "data_sinks/show", data_sink: sink
    else
      json.(sink, :id, :owner_id, :org_id, :name, :status)
      json.sink_type sink.raw_sink_type(current_user)
    end
  end

  sharers = @sharers.is_a?(Hash) ? (@sharers[data_set.id] || []) : data_set.sharers[:sharers]
  json.sharers sharers do |sharer|
    json.partial! @api_root + "data_sets/sharers/show", sharer: sharer
  end

  json.external_sharers data_set.external_sharers do |sharer|
    json.(sharer, :id, :name, :description, :email, :notified_at, :updated_at, :created_at)
  end

  json.has_custom_transform data_set.has_custom_transform?

  if (@expand)
    json.transform_id data_set.code_container_id
    # For control, when dataset is deleted, this may raise an error.
    if @transform_optional
      json.transform data_set.transform_optional
    else
      json.(data_set, :transform)
    end
    json.output_schema data_set.output_schema_with_annotations
    json.(data_set, :output_validation_schema)
    json.(data_set, :output_validator_id)
    json.child_data_sets data_set.child_data_sets, partial: @api_root + "data_sets/show_child", as: :data_set
  end

  if action_name == 'show'
    parent_source = data_set.parent_source
    json.source_config parent_source&.source_config
    json.connector_type parent_source&.connector_type
    json.partial! @api_root + "connectors/show", connector: data_set.connector
  end

  json.(data_set,
    :output_schema_validation_enabled,
    :custom_config,
    :runtime_config,
    :data_sample_id
  )

  json.(data_set, :semantic_schema_id)
  if @expand
    json.semantic_schema data_set.semantic_schema&.schema
  end

  if (@include_samples)
    max = (@max_samples_count || DataSet::Max_Cached_Samples)
    json.data_samples data_set.prepare_data_samples_with_metadata(data_set.data_samples[0..max])
  end

  json.(data_set, :endpoint_spec) if data_set.flow_type == FlowNode::Flow_Types[:api_server]

  json.flow_type data_set.flow_type

  json.(data_set, :copied_from_id, :updated_at, :created_at)

  if @tags[:data_sets].present?
    json.tags @tags[:data_sets][data_set.id]
  else
    json.tags data_set.tags_list
  end

  if (@include_summary)
    json.summary data_set.summary
  end

  json.rating data_set.main_rating
  json.rating_votes data_set.main_rating_count
  json.runtime_status data_set.runtime_status
end

if data_set.referenced_resources_enabled?
  json.referenced_resource_ids do
    data_set.referencing_fields.each do |key|
      json.set!(key, data_set.send("ref_#{key}_ids"))
    end
  end
end

if data_set.org&.marketplace_enabled?
  json.domains data_set.domains do |domain|
    json.(domain, :id, :name)
  end
end
