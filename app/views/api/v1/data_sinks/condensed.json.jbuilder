json.array! @data_sinks do |data_sink|

  json.(data_sink,
    :id,
    :origin_node_id,
    :flow_node_id,
    :owner_id,
    :org_id,
    :status,
    :runtime_status,
    :data_set_id,
    :sink_config,
    :sink_type,
    :flow_type,
    :ingestion_mode,
    :updated_at,
    :created_at
  )

  json.is_script data_sink[:code_container_id].present?
end
