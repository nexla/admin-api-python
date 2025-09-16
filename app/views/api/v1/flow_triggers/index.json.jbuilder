json.array! @flow_triggers do |flow_trigger|
  json.partial! @api_root + "flow_triggers/show", flow_trigger: flow_trigger
end