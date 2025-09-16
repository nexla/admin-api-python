class ApiKey
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This class is used
  # only to render and validate API schema for api_key objects
  # (DataSetsApiKey, UsersApiKey, etc), which all share a common schema.
end
