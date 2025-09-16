class RatingVotes::Vote < RatingVotes::Base
  attribute :vote

  validates :vote, inclusion: {in: 0..5}

  def call
    rating_vote.update!(vote: vote)
    RatingVotes::RecalculateRating.new(performer: performer, org: org, item: item, rating_type: rating_type).perform!
  end
end
