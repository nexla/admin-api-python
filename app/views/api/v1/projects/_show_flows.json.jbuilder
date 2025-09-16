json.array!(@project.flow_nodes) do |fn|
  json.(fn,
    :id,
    :project_id,
    :name,
    :description,
    :data_source_id,
    :data_set_id,
    :updated_at,
    :created_at
  )

  json.partial! @api_root + "flows/show", flows: @flows, resources: @resources, render_projects: false, flows_only: false
end

