module Api::V1
  class GenAiIntegrationConfigsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    def index
      @gen_ai_configs = GenAiConfig.where(org: current_org).page(@page).per_page(@per_page)
      authorize! :read, current_org
      set_link_header(@gen_ai_configs)
    end

    def show
      return if render_schema GenAiConfig
      @gen_ai_config = GenAiConfig.find(params[:id])
      authorize! :read, @gen_ai_config
    end

    def create
      input = (validate_body_json GenAiConfig).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @gen_ai_config = GenAiConfig.build_from_input(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json GenAiConfig).symbolize_keys
      @gen_ai_config = GenAiConfig.find(params[:id])
      authorize! :manage, @gen_ai_config
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @gen_ai_config)
      @gen_ai_config.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      apigen_config = GenAiConfig.find(params[:id])
      authorize! :manage, apigen_config
      if apigen_config.status == :active
        raise Api::V1::ApiError.new(:method_not_allowed, "Cannot delete an active Gen AI config")
      end

      apigen_config.destroy!
      head :ok
    end

    private
    def validate_user
      raise Api::V1::ApiError.new(:method_not_allowed) if current_org.nil?
      raise Api::V1::ApiError.new(:forbidden) unless current_org.has_admin_access?(current_user)
    end
  end
end