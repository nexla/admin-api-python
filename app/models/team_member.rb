class TeamMember
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This model is used only to render
  # API schema for /teams/members requests
end
