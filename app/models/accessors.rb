  class Accessors
    include Api::V1::Schema
    # NOTE, not an ActiveRecord model. This class is used only to render
    # and validate API schema for /<resource>/<resource_id>/accessors requests
  end
