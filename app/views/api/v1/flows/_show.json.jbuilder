project_ids = Array.new
flow_access_roles = nil

json.set! :flows, flows do |flow|
  flow_access_roles = flow[:node].get_access_roles(current_user, current_org) if (flows.size == 1)
  project_ids << flow[:node].project_id if flow[:node].project_id.present?
  json.partial! @api_root + "flows/show_node", node: flow, attribute: :flows
end

json.set! :triggered_flows, triggered_flows do |flow|
  json.partial! @api_root + "flows/show_node", node: flow, attribute: :triggered_flows
end

json.set! :triggering_flows, triggering_flows do |flow|
  json.partial! @api_root + "flows/show_node", node: flow, attribute: :triggering_flows
end

json.set! :linked_flows, linked_flows do |flow|
  json.partial! @api_root + "flows/show_node", node: flow, attribute: :linked_flows
end

unless flows_only
  user_ids = Array.new
  org_ids = Array.new

  resources.each do |res_type, res_list|
    next if res_type == :projects && !render_projects

    json.set! res_type, res_list do |r|
      user_ids << r.owner_id
      org_ids << r.org_id

      json.(r, :id, :owner_id, :org_id)
      if r.respond_to?(:flow_node_id)
        json.(r, :flow_node_id)
        json.(r, :origin_node_id) if (res_type != :shared_data_sets)
      end
      json.(r, :name, :description)
      json.(r, :status) if r.respond_to?(:status)
      json.(r, :runtime_status) if r.respond_to?(:runtime_status)
      json.(r, :last_run_id) if r.respond_to?(:last_run_id)

      if (res_type == :shared_data_sets)
        r.flow_shared_attributes(current_user, current_org).each do |fa|
          json.set! fa.first, fa.second
        end
      elsif r.respond_to?(:flow_attributes)
        r.flow_attributes(current_user, current_org).each do |fa|
          json.set! fa.first, fa.second
        end
      end

      case res_type
      when :data_sources
        json.node_type r.node_type
        if r.flow_type == FlowNode::Flow_Types[:rag]
          json.api_keys(r.api_keys) do |api_key|
            json.(api_key, *api_key.attributes.keys)
            json.url r.ai_web_server_url
          end
        elsif r.flow_type == FlowNode::Flow_Types[:api_server]
          json.api_keys(r.api_keys) do |api_key|
            json.(api_key, *api_key.attributes.keys)
            json.url r.api_web_server_url
          end
          json.endpoint_mappings(r.endpoint_mappings)
        end

        json.run_profile(r.run_profile) if r.adaptive_flow?
        json.run_variables(r.run_variables) if r.adaptive_flow?
      when :data_sinks
        json.node_type r.node_type
        json.run_variables(r.run_variables) if r.flow_type == FlowNode::Flow_Types[:in_memory] && r.adaptive_flow?
      end

      if res_type == :code_containers
        json.code r.code if (params[:downstream_only].truthy? && action_name == 'show') || r.ai_function_type.present?
        if r.ai_function_type.present?
          json.ai_function_type r.ai_function_type
          json.custom_config r.custom_config
        end
      end

      if res_type == :data_credentials
        if params[:downstream_only].truthy? && request.user_agent == 'Nexla/GenAI'
          json.creds_enc r.credentials_enc
          json.creds_enc_iv r.credentials_enc_iv
        end
      end

      if res_type == :data_sets
        json.node_type r.node_type
        if (params[:downstream_only].truthy? && action_name == 'show') || r.code_container&.ai_function_type.present?
          json.custom_config r.custom_config
        end
        if params[:include_samples].truthy?
          json.data_samples r.data_samples
          json.output_schema r.output_schema
        end
        if action_name == 'show'
          json.nexset_api_config r.get_nexset_api_config
          parent_source = r.parent_source
          json.source_config parent_source&.source_config
          json.connector_type parent_source&.connector_type
          json.connector parent_source&.connector&.id
        end

        json.endpoint_spec r.endpoint_spec if r.flow_type == FlowNode::Flow_Types[:api_server]

        if action_name == 'show' && params[:downstream_only].truthy?
          json.transform r.transform
          json.output_schema r.output_schema
          json.output_validation_schema r.output_validation_schema
        end
      end

      if @access_roles[res_type].present?
        json.access_roles [@access_roles[res_type][r.id]]
      else
        if (r.owner_id == current_user.id)
          json.access_roles [:owner]
        elsif (flow_access_roles.present? && flow_access_roles.include?(:admin))
          json.access_roles [:admin]
        elsif (r.respond_to?(:origin_node_id) && flow_access_roles.present?)
          # data_source, data_set, and data_sink access_roles are
          # determined by the access role on the origin node, if the
          # accessor is not the resource owner. Resource-level ACLs
          # are ignored by policy here.
          json.access_roles flow_access_roles
        else
          # These are resources for which access could be promoted to a higher
          # level by some other rule (flow-level, project-level, or resource-level)
          json.access_roles r.get_access_roles(current_user, current_org)
        end
      end

      if (res_type != :shared_data_sets && res_type != :data_credentials_groups)
        if @tags[res_type].present?
          json.tags @tags[res_type][r.id]
        else
          json.tags r.tags_list
        end

        json.(r, :copied_from_id)
      end

      json.(r, :created_at, :updated_at)
    end
  end

  org_ids = org_ids.uniq.compact
  if org_ids.empty?
    json.set! :orgs, []
  else
    attrs = [ :id, :name, :description, :email_domain, :email ]
    orgs = Org.where(id: org_ids).select(attrs)
    json.orgs(orgs) do |o|
      json.(o, *attrs)
    end
  end

  user_ids = user_ids.uniq.compact
  if user_ids.empty?
    json.set! :users, []
  else
    users = User.where(id: user_ids).select(:id, :email, :full_name)
    json.users(users) do |u|
      json.(u, :id, :email)
      json.name u.full_name
    end
  end

  if render_projects
    if @projects.empty?
      json.set! :projects, []
    else
      json.projects(@projects) do |p|
        next if !project_ids.include?(p.id)
        json.(p, :id, :owner_id, :org_id, :name, :description)
        if @access_roles[:projects].present?
          json.access_roles [@access_roles[:projects][p.id]]
        else
          json.access_roles p.get_access_roles(current_user, current_org)
        end
      end
    end
  end
end
