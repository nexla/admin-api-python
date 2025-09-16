module Api::V1::DocContainers
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DocContainer) }
  end
end
