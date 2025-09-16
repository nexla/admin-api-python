json.array!(@project.flow_nodes) do |fn|
  # FN backwards-compatibility
  json.(fn,
    :id,
    :project_id,
    :data_source_id,
    :data_set_id
  )
  json.set! :data_sink_id, nil
  json.(fn,
    :updated_at,
    :created_at
  )
end