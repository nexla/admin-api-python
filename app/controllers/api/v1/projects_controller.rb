module Api::V1  
  class ProjectsController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern

    # TODO: change value once pagination is enforced again.
    # Default_Per_Page = 20
    Default_Per_Page = PAGINATE_ALL_COUNT

    def index
      @projects = add_request_filters(
        current_user.projects(current_org, access_role: request_access_role), Project
      ).page(@page).per_page(@per_page)
      set_link_header(@projects)
    end

    def create
      input = (validate_body_json Project).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @project = Project.build_from_input(api_user_info, input)
      render "show"
    end

    def show
      return if render_schema Project
      @project = Project.find(params[:id])
      authorize! :read, @project
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      project = Project.find(params[:project_id])
      authorize! :manage, project
      api_user_info = ApiUserInfo.new(current_user, current_org, input, project)
      @project = project.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json Project).symbolize_keys
      @project = Project.find(params[:id])
      authorize! :manage, @project
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @project)
      @project.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      project = Project.find(params[:id])
      authorize! :manage, project
      project.destroy
      head :ok
    end

    def flows
      @per_page = Default_Per_Page if !params[:per_page].present?
      @paginate = true
      @project = Project.find(params[:project_id])
      mode = params[:mode].to_sym

      if mode == :list
        authorize! :read, @project
      else
        authorize! :manage, @project
      end

      if (mode == :remove && request.raw_post.empty?)
        # A remove request with no input means
        # delete all data flow assocations
        input = { :flows => [] }
        mode = :reset
      elsif (mode != :list)
        input = (validate_body_json Project).symbolize_keys
      end
      input ||= {}

      if input.key?(:data_flows)
        # FN backwards-compatibility, convert to :flows
        input[:flows] = convert_data_flows_input(input[:data_flows])
        input.delete(:data_flows)
      end

      if (mode != :list)
        if (!input[:flows].is_a?(Array))
          raise Api::V1::ApiError.new(:bad_request, "Input flows attribute missing or invalid")
        end
      end
      @api_user_info = ApiUserInfo.new(current_user, current_org, input, @project)

      case mode.to_sym
      when :add
        @project.update_flows(input[:flows], @api_user_info)
      when :reset
        @project.reset_flows(input[:flows], @api_user_info)
      when :remove
        @project.remove_flows(input[:flows])
      end

      origin_nodes = @project.flow_nodes.page(@page).per_page(@per_page)
      set_link_header(origin_nodes)
      render_flows(@project, origin_nodes, params[:backwards_compatible].truthy?)
    end

    def flows_search
      @project = Project.find(params[:id])
      authorize! :read, @project

      origin_nodes = ::Flows::Search::FlowSearchExecutor.new(current_user, current_org, params[:filters], request_access_role, @project.id).call
      origin_nodes = origin_nodes.page(@page).per_page(@per_page)

      set_link_header(origin_nodes)
      render_flows(@project, origin_nodes, false)
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @projects = ResourceTagging.search_by_tags(Project, input, current_user, request_access_role, current_org)
      set_link_header(@projects)
      render "index"
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      @projects = current_user.projects(current_org, { access_role: request_access_role, access_roles: @access_roles })
      @projects = Common::Search::BasicSearchExecutor.new(current_user, current_org, Project, params[:filters], @projects, sort_opts: sort_opts).call
      @projects = @projects.page(@page).per_page(@per_page)

      set_link_header(@projects)
      render :index
    end

    protected

    def convert_data_flows_input (data_flows)
      # FN backwards-compatibility
      flow_node_ids = Array.new

      data_flows.each do |df|
        df.symbolize_keys!
        res_key = (df.keys & DataFlow::Resource_Keys).first
        next if (res_key.nil? || df[res_key].blank?)
        model = res_key.to_s.gsub("_id", "").camelcase.constantize
        res = model.find_by_id(df[res_key])
        if res.nil?
          raise Api::V1::ApiError(:not_found,
            "Flow resource not found: #{res.class.name}, #{df[res_key]}")
        end
        if (!res.respond_to?(:flow_node_id) || res.flow_node_id.nil?)
          raise Api::V1::ApiError(:bad_request,
            "Invalid flow resource : #{res.class.name}, #{df[res_key]}")
        end
        flow_node_ids << res.flow_node_id
      end

      return flow_node_ids
    end

    def render_flows (project, origin_nodes, backwards_compatible = false)
      @flows_only = false

      if current_org.present?
        is_org_admin = current_org.has_admin_access?(current_user)
      else
        is_org_admin = false
      end

      project_role = project.get_access_role(current_user)

      if backwards_compatible
        flows = []
        origin_nodes.each do |fn|
          flows << {
            id: fn.id,
            project_id: @project.id,
            data_source_id: fn.data_source_id,
            data_set_id: fn.data_set_id,
            data_sink_id: fn.data_sink_id,
            updated_at: @project.updated_at,
            created_at: @project.created_at
          }
        end
        render :json => flows, :status => :ok
      else
        @flows = Array.new
        @resources = FlowNode.empty_flow
        @resources.delete(:flow)
        @projects = [@project]

        origin_nodes.each do |fn|
          @flows << fn.flow(@api_user_info)
          next if @flows_only
          r = fn.resources(@api_user_info)
          @resources.keys.each do |k|
            @resources[k] += r[k].to_a if r[k].present?
          end
        end

        unless @flows_only
          @access_roles = Hash.new
          @resources.keys.each do |k|
            @resources[k].uniq!
            @resources[k].each do |r|
              @access_roles[k] ||= Hash.new
              ar = project_role
              if (r.owner_id == current_user.id)
                ar = :owner
              elsif is_org_admin
                ar = :admin
              end
              @access_roles[k][r.id] = ar
            end
          end
          load_tags(@access_roles)
        end

        render 'api/v1/flows/show'
      end
    end
  end
end