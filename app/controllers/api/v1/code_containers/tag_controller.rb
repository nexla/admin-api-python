module Api::V1::CodeContainers
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(CodeContainer) }
  end
end
