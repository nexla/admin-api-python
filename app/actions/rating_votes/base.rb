class RatingVotes::Base < BaseAction
  attribute :item
  attribute :rating_type

  validate :in_org_scope

  private

  memoize def rating_vote
    RatingVote.find_or_initialize_by(user: performer, item: item, rating_type: rating_type)
  end

  def in_org_scope
    return if item&.org_id == org.id

    errors.add(:item, "not belongs to Org")
  end
end
