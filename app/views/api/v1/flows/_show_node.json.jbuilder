json.(node[:node],
  :id,
  :parent_node_id,
  :origin_node_id,
  node[:node].resource_key
)

json.(node[:node], :runtime_status) if node[:node].respond_to?(:runtime_status)
json.(node[:node], :node_type) if node[:node].respond_to?(:node_type)

if node[:node].is_origin?
  if node[:node].shared_origin_node_id.present?
    json.shared_data_set_id node[:node].parent_node&.data_set_id
  end

  json.(node[:node],
    :status,
    :project_id,
    :flow_type,
    :name,
    :description,
    :managed,
    :ingestion_mode
  )

  json.(node[:node], :data_retriever_ids) if attribute.present? && attribute == :linked_flows && node[:node].respond_to?(:data_retriever_ids)
  json.(node[:node], :last_run_id)
end

if !@origin_only
  unless node[:linked_flows].nil?
    json.linked_flows node[:linked_flows]
  end

  unless node[:flow_triggers].nil?
    json.set! :flow_triggers, node[:flow_triggers] do |flow_trigger|
      json.triggering_origin_node_id flow_trigger.triggering_flow_node.origin_node_id
      json.(flow_trigger, :triggered_origin_node_id, :triggering_flow_node_id, :triggering_event_type, :triggered_event_type)
      json.triggering_resource_type flow_trigger.triggering_resource.class.name.underscore
      json.triggering_resource_id flow_trigger.triggering_resource.id
      json.triggered_resource_type flow_trigger.triggered_resource.class.name.underscore
      json.triggered_resource_id flow_trigger.triggered_resource.id
    end
  end

  json.set! :children, node[:children] do |child_node|
    json.partial! @api_root + "flows/show_node", node: child_node, attribute: :children
  end
end
