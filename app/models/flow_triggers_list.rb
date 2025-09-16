class FlowTriggersList
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This class is used only to render
  # API schema for /data_sources/{id}/flow_triggers and
  # /data_sinks/{id}/flow_triggers requests
end
