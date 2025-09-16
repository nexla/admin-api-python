module Api::V1::Transforms
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(Transform) }
  end
end
