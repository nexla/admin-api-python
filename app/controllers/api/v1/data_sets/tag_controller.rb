module Api::V1::DataSets
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(DataSet) }
  end
end
