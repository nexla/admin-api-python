class RunProfileParameters
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This model is used only to render
  # API schema for /flows/:flow_id/run_profiles/activate requests
end
