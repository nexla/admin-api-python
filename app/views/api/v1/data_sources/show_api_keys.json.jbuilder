json.array! @api_keys do |api_key|
  if [FlowNode::Flow_Types[:rag], FlowNode::Flow_Types[:api_server]].include?(@resource.flow_type)
    json.(api_key, *api_key.attributes.keys)
    json.url @resource.ai_web_server_url
  else
    json.(api_key,
      :id,
      :owner_id,
      :org_id,
      @resource_attribute,
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
end
