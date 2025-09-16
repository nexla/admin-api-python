module Api::V1  
  class DocContainersController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include AccessorsConcern

    def index
      @doc_containers = current_user.doc_containers(request_access_role, current_org)
        .page(@page).per_page(@per_page)
      set_link_header(@doc_containers)
    end
    
    def show
      return if render_schema DocContainer
      @doc_container = DocContainer.find(params[:id])
      authorize! :read, @doc_container
    end

    def create
      input = (validate_body_json DocContainer).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @doc_container = DocContainer.build_from_input(api_user_info, input)    
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_doc_container = DocContainer.find(params[:doc_container_id])
      authorize! :manage, copied_doc_container
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_doc_container)
      @doc_container = copied_doc_container.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json DocContainer).symbolize_keys
      @doc_container = DocContainer.find(params[:id])
      authorize! :manage, @doc_container
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @doc_container)
      @doc_container.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      doc_container = DocContainer.find(params[:id])
      authorize! :manage, doc_container
      doc_container.destroy
      head :ok
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @doc_containers = ResourceTagging.search_by_tags(DocContainer, input, current_user, request_access_role, current_org)
      set_link_header(@doc_containers)
      render "index"
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      scope = current_user.doc_containers(request_access_role, current_org)
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, DocContainer, params[:filters], scope, sort_opts: sort_opts).call
      @doc_containers = scope.page(@page).per_page(@per_page)
      set_link_header(@doc_containers)
      render :index
    end

  end
end


