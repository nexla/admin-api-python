json.(flow_node,
  :id,
  :parent_node_id,
  :origin_node_id
)

if flow_node.shared_origin_node_id.present?
  json.(flow_node, :shared_origin_node_id)
  json.shared_data_set_id flow_node.parent_node&.data_set_id
end

json.partial! @api_root + "users/owner", user: flow_node.owner
json.partial! @api_root + "orgs/brief", org: flow_node.org

json.(flow_node, 
  :name,
  :description,
  :status,
  :cluster_id,
  :flow_type
)

json.access_roles flow_node.get_access_roles(current_user, current_org)

json.(flow_node,
  :copied_from_id,
  :updated_at,
  :created_at
)

json.partial! @api_root + "flow_nodes/resources", flow_node: flow_node

#json.tags flow_node.tags_list