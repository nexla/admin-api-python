module Api::V1
  class DashboardTransformsController < Api::V1::ApiController
    include Api::V1::ApiKeyAuth

    using Refinements::HttpResponseString

    skip_before_action :authenticate, only: [:get_flows_dashboard]
    before_action only: [:get_flows_dashboard] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end
    before_action :initialize_date_params, only: [:account_metrics]

    METRICS_RESOURCES_MAP = {
      "data_sources" => "sources",
      "data_sets" => "datasets",
      "data_sinks" => "sinks"
    }

    def index
      transforms_select = current_user.dashboard_transforms(request_access_role, current_org)
      @dashboard_transform = add_request_filters(transforms_select, DashboardTransform).page(@page).per_page(@per_page)
      set_link_header(@dashboard_transform)
    end

    def index_all
      head :forbidden and return if !current_user.super_user?
      @dashboard_transform = add_request_filters(DashboardTransform, DashboardTransform).page(@page).per_page(@per_page)

      set_link_header(@dashboard_transform)
      render "index"
    end

    def show
      return if render_schema DashboardTransform
      resource_type, id = get_resource_info
      @dashboard_transform = DashboardTransform.where(:resource_type => resource_type, :resource_id => id).first
      raise Api::V1::ApiError.new(:not_found) if @dashboard_transform.nil?
      authorize! :read, @dashboard_transform
    end

    def create
      input = (validate_body_json DashboardTransform).symbolize_keys
      resource_type, id = get_resource_info
      input[:resource_type] = resource_type
      input[:resource_id] = id
      @dashboard_transform = DashboardTransform.build_from_input(input, current_user, current_org)
      render "show"
    end

    def update
      input = (validate_body_json DashboardTransform).symbolize_keys
      resource_type, id = get_resource_info
      @dashboard_transform = DashboardTransform.where(:resource_type => resource_type, :resource_id => id).first
      raise Api::V1::ApiError.new(:not_found) if @dashboard_transform.nil?
      authorize! :manage, @dashboard_transform

      @dashboard_transform.update_mutable!(request, current_user, current_org, input)
      render "show"
    end

    def destroy
      resource_type, id = get_resource_info
      dashboard_transform = DashboardTransform.where(:resource_type => resource_type, :resource_id => id).first
      raise Api::V1::ApiError.new(:not_found) if dashboard_transform.nil?
      authorize! :manage, dashboard_transform
      dashboard_transform.destroy
      head :ok
    end

    def get_flows_dashboard
      model = params[:model]
      id_param = (model.name.singularize.underscore + "_id").to_sym
      resource = model.find(params[id_param])
      authorize! :read, resource
      result = FlowsDashboardService.new.get_flows_dashboard(resource, params)
      render :json => result, :status => result[:status]
    end

    def flows_status_metrics
      # Get dashboard metrics
      metrics_result = get_flow_dashboard_metrics
      unless metrics_result[:status].to_s.success_code?
        return render :json => metrics_result, :status => metrics_result[:status]
      end

      @status = metrics_result[:status]
      @dashboard_metrics = metrics_result[:metrics].try(:stringify_keys) || {}
      @flows_org = org_for_flow_metrics
      origin_node_ids = gather_origin_node_ids(@flows_org, @dashboard_metrics)

      # Get flows
      @per_page = Api::V1::FlowsController::Default_Per_Page unless params[:per_page].present?
      api_user_info = ApiUserInfo.new(current_user, @flows_org)

      options = {
        access_role: request_access_role(:owner),
        access_roles: @access_roles
      }
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      origin_nodes = FlowNode.where(id: origin_node_ids) # notice - no permissions check here. This is intentional. We rely on backend which should hold only the data that user has access to.
      unless @flows_org.has_admin_access?(current_user)
        origin_nodes = current_user.origin_nodes(@flows_org, options.merge(selected_ids: origin_node_ids))
      end
      origin_nodes = add_request_filters(origin_nodes, FlowNode).page(@page).per_page(@per_page)

      render_result = Flows::Builders::RenderBuilder.new(api_user_info, origin_nodes, options, false).build
      filter_out_metrics(render_result[:resources], @dashboard_metrics)

      @flows, @resources, @projects = render_result.values_at(:flows, :resources, :projects)

      load_tags(@access_roles)
      set_link_header(origin_nodes)

      render template: @api_root + "flows/show"
    end

    def account_metrics
      # Supported models are User and Org. The model passed
      # in represents the primary resource for the endpoint.
      # If the primary is User, an additional org_id parameter 
      # can be passed to limit the returned metrics to that
      # org scope.

      model = params[:model]
      id_param = (model.name.singularize.underscore + "_id").to_sym
      resource = model.find(params[id_param])

      raise Api::V1::ApiError.new(:bad_request,
                                  "Invalid 'from' date format for account metrics") if params[:from].nil?

      if resource.is_a?(Org)
        authorize! :write, resource
      elsif resource.is_a?(User)
        authorize! :read, resource
        if params.key?(:org_id)
          org = Org.find(params[:org_id])
          raise Api::V1::ApiError.new(:forbidden) if !resource.org_member?(org)
        end
      else
        raise Api::V1::ApiError.new(:internal_server_error,
          "Invalid resource type for account metrics")
      end

      if params[:aggregate].truthy?
        result = FlowsDashboardService.new.get_metric_data(resource, params)
      else
        result = MetricsService.new.get_account_metrics(resource, params)
      end

      render :json => result, :status => result[:status]
    end

    def get_resource_info
      model = params[:model]
      id = "#{model.name.underscore.singularize}_id".to_sym

      resource_type = QuarantineSetting::Resource_Types[model.name.underscore.singularize.to_sym]
      return resource_type, params[id]
    end

    protected

    def get_flow_dashboard_metrics
      @model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      id_param = (@model.name.singularize.underscore + "_id").to_sym
      @resource = @model.find(params[id_param])
      authorize! :read, @resource

      resource_for_metrics = @resource
      org = nil
      if !@resource.is_a?(Org) && !current_user.super_user?
        org = @resource.try(:org) || @resource.try(:default_org)
      end
      org ||= current_org

      user = nil
      if @resource.is_a?(User)
        user = @resource
      else
        user = @resource.owner
      end

      access_role = request_access_role(:owner)
      if access_role == :owner
        resource_for_metrics = user
      elsif request_access_role == :collaborator
        resource_for_metrics = org
      end

      FlowsDashboardService.new.get_flows_dashboard(resource_for_metrics, params)
    end

    def org_for_flow_metrics
      org = @model == Org ? @resource : current_org

       # routes for dashboard trigger impersonation logic which breaks selecting correct @api_org, leaving it blank
      return current_user.default_org || current_user.orgs.first unless org

      org
    end

    def gather_origin_node_ids(org, dashboard_metrics)
      data_source_ids = dashboard_metrics['sources']&.keys || []
      data_sets_ids = dashboard_metrics['datasets']&.keys || []
      data_sink_ids = dashboard_metrics['sinks']&.keys || []

      origin_node_ids = DataSource.where(id: data_source_ids.map(&:to_i), org_id: org&.id)
        .pluck(:origin_node_id)

      origin_node_ids += DataSet.where(id: data_sets_ids.map(&:to_i), org_id: org&.id)
        .pluck(:origin_node_id)

      origin_node_ids += DataSink.where(id: data_sink_ids.map(&:to_i), org_id: org&.id)
        .pluck(:origin_node_id)

      origin_node_ids.compact.uniq
    end

    def filter_out_metrics(resources_hash, metrics)
      data_sources_ids = resources_hash[:data_sources].map(&:id)
      data_sets_ids = resources_hash[:data_sets].map(&:id)
      data_sinks_ids = resources_hash[:data_sinks].map(&:id)

      metrics["sources"]&.delete_if{|k, v| !data_sources_ids.include?(k.to_i) }
      metrics["datasets"]&.delete_if{|k, v| !data_sets_ids.include?(k.to_i) }
      metrics["sinks"]&.delete_if{|k, v| !data_sinks_ids.include?(k.to_i) }
    end
  end
end
