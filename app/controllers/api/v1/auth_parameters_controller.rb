module Api::V1
  class AuthParametersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      @auth_parameters = add_request_filters(AuthParameter.jit_preload, AuthParameter)
        .page(@page).per_page(@per_page)
      set_link_header(@auth_parameters)
    end

    def show
      return if render_schema AuthParameter
      @auth_param = AuthParameter.find(params[:id])
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json AuthParameter).symbolize_keys
      @auth_param = AuthParameter.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json AuthParameter).symbolize_keys
      @auth_param = AuthParameter.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @auth_param.nil?

      @auth_param.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      auth_param = AuthParameter.find(params[:id])
      auth_param.destroy
      head :ok
    end

  end
end