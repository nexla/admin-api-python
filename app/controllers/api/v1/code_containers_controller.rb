module Api::V1  
  class CodeContainersController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern
  
    before_action only: [:index, :error_functions, :search] do
      if params.key?(:reusable)
        @all = params[:reusable] == "all"
        @reusable = params[:reusable].truthy?
      else
        @all = true
      end
    end

    def public
      @code_containers = add_request_filters(CodeContainer.where(:public => true), CodeContainer)
        .page(@page).per_page(@per_page)
      set_link_header(@code_containers)
      render "index"
    end

    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      scope = current_user.code_containers(current_org, options)

      if @all
        scope = with_public(scope)
      else
        scope = @reusable ? with_public(scope.reusable) : scope.not_reusable
      end

      scope = add_request_filters(scope, CodeContainer)
      @code_containers = scope.page(@page).per_page(@per_page)

      set_link_header(@code_containers)
      load_tags(@access_roles)
    end

    def show
      return if render_schema CodeContainer
      @code_container = CodeContainer.find(params[:id])
      authorize! :read, @code_container
    end

    def create
      input = (validate_body_json CodeContainer).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @code_container = CodeContainer.build_from_input(api_user_info, input)    
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_code_container = CodeContainer.find(params[:code_container_id])
      authorize! :manage, copied_code_container
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_code_container)
      @code_container = copied_code_container.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json CodeContainer).symbolize_keys
      @code_container = CodeContainer.find(params[:id])
      authorize! :manage, @code_container
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @code_container)
      @code_container.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      code_container = CodeContainer.find(params[:id])
      authorize! :manage, code_container
      code_container.destroy
      head :ok
    end

    def repo
      @code_container = CodeContainer.find(params[:code_container_id])
      authorize! :read, @code_container
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @code_containers = ResourceTagging.search_by_tags(CodeContainer, input, current_user, request_access_role, current_org)
      set_link_header(@code_container)
      render "index"
    end

    def error_functions
      selected = @all ?
        current_user.error_transforms(request_access_role, current_org) :
        @reusable ?
          current_user.error_transforms(request_access_role, current_org).reusable :
          current_user.error_transforms(request_access_role, current_org).not_reusable

      @code_containers = add_request_filters(selected, CodeContainer).page(@page).per_page(@per_page)
      set_link_header(@code_containers)
      render "index"
    end

    def search
      scope = current_user.code_containers(current_org, { access_role: request_access_role, access_roles: @access_roles })
      unless @all
        scope = @reusable ? scope.reusable : scope.not_reusable
      end

      include_public = params[:include_public].truthy? && (@all || @reusable)

      sort_opts = params.slice(:sort_by, :sort_order)
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, CodeContainer, params[:filters], scope, include_public: include_public, sort_opts: sort_opts).call
      @code_containers = scope.page(@page).per_page(@per_page)
      set_link_header(@code_containers)

      load_tags(@access_roles)
      render :index
    end

    private
    def with_public(scope)
      return scope unless params[:include_public].truthy?

      scope.or( CodeContainer.where(public: true).select( scope.select_values ) )
    end
  end
end


