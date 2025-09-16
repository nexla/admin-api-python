class RatingVote < ApplicationRecord
  include Api::V1::Schema

  belongs_to :user
  belongs_to :item, polymorphic: true
end
