json.array! @flow_triggers.each do |flow_trigger|
  json.partial! @api_root + 'flow_triggers/show', flow_trigger: flow_trigger
end
