json.(flow_trigger, :id)
 
json.partial! @api_root + "users/owner", user: flow_trigger.owner
json.partial! @api_root + "orgs/brief", org: flow_trigger.org

json.(flow_trigger, :status, :triggering_event_type, :triggering_origin_node_id, :triggering_flow_node_id)
json.triggering_resource_type flow_trigger.triggering_resource.class.name.underscore
json.triggering_resource_id flow_trigger.triggering_resource.id

json.(flow_trigger, :triggered_event_type, :triggered_origin_node_id)
json.triggered_resource_type flow_trigger.triggered_resource.class.name.underscore
json.triggered_resource_id flow_trigger.triggered_resource.id

json.(flow_trigger, :updated_at, :created_at)

#json.access_roles flow_trigger.get_access_roles(@api_user, @api_org)

