module Api::V1::DataMaps
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataMap) }
  end
end
