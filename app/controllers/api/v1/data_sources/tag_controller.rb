module Api::V1::DataSources
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataSource) }
  end
end
