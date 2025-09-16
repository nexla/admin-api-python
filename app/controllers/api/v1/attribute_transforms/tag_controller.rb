module Api::V1::AttributeTransforms
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(AttributeTransform) }
  end
end
