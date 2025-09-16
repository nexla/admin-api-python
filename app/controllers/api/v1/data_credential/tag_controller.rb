module Api::V1::DataCredential
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataCredentials) }
  end
end
