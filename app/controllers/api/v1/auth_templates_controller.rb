module Api::V1
  class AuthTemplatesController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      if params.key?(:auth_template_name)
        return show
      end
      @auth_templates = add_request_filters(AuthTemplate.jit_preload, AuthTemplate)
        .page(@page).per_page(@per_page)
      set_link_header(@auth_templates)
      render "index"
    end

    def show
      return if render_schema AuthTemplate
      if params.key?(:auth_template_name)
        @auth_template = AuthTemplate.find_by_name!(params[:auth_template_name])
      else
        @auth_template = AuthTemplate.find(params[:id])
      end
      render "show"
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json AuthTemplate).symbolize_keys
      @auth_template = AuthTemplate.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json AuthTemplate).symbolize_keys
      if params.key?(:auth_template_name)
        @auth_template = AuthTemplate.find_by_name!(params[:auth_template_name])
      else
        @auth_template = AuthTemplate.find(params[:id])
      end
      @auth_template.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      if params.key?(:auth_template_name)
        auth_template = AuthTemplate.find_by_name!(params[:auth_template_name])
      else
        auth_template = AuthTemplate.find(params[:id])
      end
      auth_template.destroy
      head :ok
    end

  end
end