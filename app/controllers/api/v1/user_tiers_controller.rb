module Api::V1
  class UserTiersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      head :forbidden and return if !current_user.super_user?
      @user_tier = UserTier.all.page(@page).per_page(@per_page)
      set_link_header(@user_tier)
    end

    def show
      return if render_schema UserTier

      @user_tier = UserTier.find(params[:id])
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json UserTier).symbolize_keys
      @user_tier = UserTier.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json UserTier).symbolize_keys
      @user_tier = UserTier.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @user_tier.nil?

      @user_tier.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      user_tier = UserTier.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if user_tier.nil?
      user_tier.destroy
      head :ok
    end

  end
end