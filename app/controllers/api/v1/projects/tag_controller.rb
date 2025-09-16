module Api::V1::Projects
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(Project) }
  end
end
