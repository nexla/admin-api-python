class DataSetSharer
  include Api::V1::Schema
  # NOTE, not an ActiveRecord model. This model is used only to render/validate
  # API schema for /data_sets/<data_set_id>/sharer requests
end
