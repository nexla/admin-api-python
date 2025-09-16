module Api::V1::CustomDataFlows
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(CustomDataFlow) }
  end
end
