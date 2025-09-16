module Api::V1::DataSchemas
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataSchema) }
  end
end
