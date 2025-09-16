class RatingVotes::Unvote < RatingVotes::Base
  validate :vote_exists

  def call
    rating_vote.destroy!
    RatingVotes::RecalculateRating.new(performer: performer, org: org, item: item, rating_type: rating_type).perform!
  end

  private

  def vote_exists
    return if rating_vote.persisted?

    errors.add(:item, 'not rated by the user')
  end
end
