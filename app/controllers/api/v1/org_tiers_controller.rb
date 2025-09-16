module Api::V1
  class OrgTiersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      head :forbidden and return if !current_user.super_user?
      @org_tier = OrgTier.all.page(@page).per_page(@per_page)
      set_link_header(@org_tier)
    end

    def show
      return if render_schema OrgTier

      @org_tier = OrgTier.find(params[:id])
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json OrgTier).symbolize_keys
      @org_tier = OrgTier.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json OrgTier).symbolize_keys
      @org_tier = OrgTier.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @org_tier.nil?

      @org_tier.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      org_tier = OrgTier.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if org_tier.nil?
      org_tier.destroy
      head :ok
    end

  end
end