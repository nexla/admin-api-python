class QueryParameter
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This model is used only to validate
  # request query parameters using the API schema validator mechanism. 
end