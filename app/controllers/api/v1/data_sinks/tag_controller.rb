module Api::V1::DataSinks
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataSink) }
  end
end
