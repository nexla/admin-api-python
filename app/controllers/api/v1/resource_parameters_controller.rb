module Api::V1
  class ResourceParametersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      @resource_parameters = add_request_filters(ResourceParameter.jit_preload, ResourceParameter)
        .page(@page).per_page(@per_page)
      set_link_header(@resource_parameters)
    end

    def show
      return if render_schema ResourceParameter
      @resource_param = ResourceParameter.find(params[:id])
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json ResourceParameter).symbolize_keys
      @resource_param = ResourceParameter.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json ResourceParameter).symbolize_keys
      @resource_param = ResourceParameter.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @resource_param.nil?

      @resource_param.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      resource_param = ResourceParameter.find(params[:id])
      resource_param.destroy
      head :ok
    end

  end
end