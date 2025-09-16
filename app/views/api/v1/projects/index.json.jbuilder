json.array! @projects do |project|
  json.(project, :id)

  json.owner do
    json.(project.owner, :id, :full_name, :email)
  end

  if project.org.nil?
    json.org nil
  else
    json.org do
      json.(project.org, :id, :name, :email_domain, :client_identifier)
    end
  end

  json.(project,
    :name,
    :description,
    :client_identifier,
    :client_url
  )

  if @expand
    json.data_flows project.flow_nodes do |fn|
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

    json.flows project.flow_nodes do |fn|
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
    end
  end

  json.flows_count project.flow_nodes.count

  json.access_roles project.get_access_roles(current_user, current_org)
  json.tags project.tags_list

  json.(project,
    :copied_from_id,
    :updated_at,
    :created_at
  )
end