class RatingVotes::RecalculateRating < RatingVotes::Base
  def call
    item.update!(**item_attributes)
  end

  private

  def item_attributes
    rating, count = item.send("#{rating_type}_ratings").pluck(Arel.sql('AVG(vote), COUNT(*)'))[0]

    {:"#{rating_type}_rating" => rating, :"#{rating_type}_rating_count" => count}
  end
end
