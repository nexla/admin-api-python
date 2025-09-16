module Api::V1::Validators
  class TagController < Api::V1::TagController
    before_action -> { setup_tagging_service(Validator) }
  end
end
