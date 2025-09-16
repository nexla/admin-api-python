module Api::V1  
  class RatingVotesController < Api::V1::ApiController      
    include AccessorsConcern
    
    def vote
      action = RatingVotes::Vote.new(vote: user_input['vote'], **base_attributes)
      action.perform!
      
      render :current_rating
    end

    def unvote
      RatingVotes::Unvote.new(**base_attributes).perform!

      render :current_rating
    end

    private

    memoize def current_item
      params[:model].find(params[parameter_name])
    end

    def parameter_name
      params[:custom_id].presence || :item_id
    end

    memoize def user_input
      validate_body_json(RatingVote)
    end

    helper_method def rating_type
      user_input['rating_type'].to_sym
    end

    helper_method def rating
      current_item.ratings(rating_type)
    end

    def base_attributes
      {
        performer: current_user,
        org: current_org,
        rating_type: rating_type,
        item: current_item
      }
    end
  end
end


