json.array!(@flow_nodes) do |flow_node|
  json.(flow_node, *FlowNode::Condensed_Select_Fields)
end
