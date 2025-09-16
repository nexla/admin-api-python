module Api::V1
  class DataSinksController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include ApiKeyConcern
    include DocsConcern
    include Api::V1::ApiKeyAuth
    include AccessorsConcern
    include ControlEventConcern

    skip_before_action :authenticate, only: [:run_status]
    before_action only: [:run_status] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      # NOTE most_recent_limit is a workaround for environments where
      # the total resource (data_source, data_sink, flow_nodes, etc) 
      # counts visible to the caller can be in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on data_sinks
      # list views.
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      @data_sinks = add_request_filters(
        current_user.data_sinks(current_org, options), DataSink
      ).page(@page).per_page(@per_page).preload(:taggings)
      set_link_header(@data_sinks)

      if @expand
        DataMap.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
        DataCredentials.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
      end

      load_tags(@access_roles)
    end

    def index_all
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      params[:org_id] = current_org&.id unless current_user.super_user?
      @data_sinks = add_request_filters(DataSink.jit_preload, DataSink)
      @data_sinks = @data_sinks.in_dataplane(request_dataplane) if request_dataplane.present?
      @data_sinks = @data_sinks.page(@page).per_page(@per_page)
      set_link_header(@data_sinks)
      render "index"
    end

    def index_all_condensed
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      sort_opts = {}
      filter_opts = {}
      filter_opts[:status] = params[:status].upcase if params[:status].is_a?(String)
      if params[:org_id].present?
        filter_opts[:org_id] = params[:org_id].to_i
      elsif request_dataplane.present?
        filter_opts[:org_id] = Org.in_dataplane(request_dataplane).pluck(:id)
      end
      resource_id = params[:resource_id].to_i
      filter_opts[:id] = resource_id.next..Float::INFINITY unless resource_id.zero?
      filter_opts[:org_id] = current_org&.id unless current_user.super_user?
      sort_opts[params[:sort_by].presence || :id] = params[:sort_order].presence || :asc if params[:sort_by].is_a?(String)

      @data_sinks = DataSink.all_condensed(filter_opts, sort_opts)
        .page(@page).per_page(@per_page)
      set_link_header(@data_sinks)
      render "condensed"
    end

    def index_all_by_data_set
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      data_sinks = DataSink.all_by_data_set(params[:status], request_dataplane)
        .paginate(:page => @page, :per_page => @per_page)
      set_link_header(data_sinks)
      render :json => data_sinks
    end

    def index_all_ids
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      cnd = {}
      cnd[:status] = params[:status] if !params[:status].blank?
      if params[:org_id].present?
        cnd[:org_id] = params[:org_id].to_i
      elsif request_dataplane.present?
        cnd[:org_id] = Org.in_dataplane(request_dataplane).pluck(:id)
      end
      cnd[:org_id] = current_org&.id unless current_user.super_user?
      render json: DataSink.where(cnd).pluck(:id)
    end

    def show
      return if render_schema DataSink
      @data_sink = DataSink.find(params[:id])
      authorize! :read, @data_sink
    end

    def create
      input = (validate_body_json DataSink).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_sink = DataSink.build_from_input(api_user_info, input, request)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_data_sink = DataSink.find(params[:data_sink_id])
      authorize! :manage, copied_data_sink
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_data_sink)
      @data_sink = copied_data_sink.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json DataSink).symbolize_keys

      @data_sink = DataSink.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_sink.nil?
      authorize! :manage, @data_sink

      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_sink)
      @data_sink.update_mutable!(api_user_info, input, request)
      render "show"
    end

    def destroy
      data_sink = DataSink.find(params[:id])
      authorize! :manage, data_sink
      data_sink.destroy
      head :ok
    end

    def activate
      @data_sink = DataSink.find(params[:data_sink_id])
      authorize! :operate, @data_sink
      params[:activate] ? @data_sink.activate! : @data_sink.pause!
      render "show"
    end

    def metrics
      @data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, @data_sink
      result = MetricsService.new.get_metric_data(@data_sink, params)
      render :json => result, :status => result[:status]
    end

    def get_offset
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink
      result = ControlService.new(data_sink).get_offset(params[:data_set_id])
      render :json => result, :status => result[:status]
    end

    def get_quarantine_offset
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink
      result = ControlService.new(data_sink).get_quarantine_offset(params[:data_set_id])
      render :json => result, :status => result[:status]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_sinks = ResourceTagging.search_by_tags(DataSink, input, current_user, request_access_role, current_org)
      set_link_header(@data_sinks)
      render "index"
    end

    def validate_config
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink
      config = (!request.raw_post.blank?) ? request.raw_post : data_sink.sink_config
      result = ControlService.new(data_sink).validate_config(config, data_sink.connector.connection_type)
      render :json => result, :status => result[:status]
    end

    def run_status
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink
      api_user_info = ApiUserInfo.new(current_user, current_org, {}, data_sink)
      result = data_sink.run_status(api_user_info, params[:run_id])
      render status: result[:status], json: result
    end

    def run_analysis
      data_sink = DataSink.find(params[:data_sink_id])
      authorize! :read, data_sink

      result = MonitorService.new.run_sink_monitor(current_org, data_sink,
        request.headers[:Authorization], params[:include_flow_lags].truthy?)
      render :json => result, :status => result[:status]
    end

    def script_sink_config
      # Informational endpoint to help verify data_sink
      # script config for a given org/cluster without
      # having to create a new data_sink.
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      params[:org_id] = current_org&.id unless current_user.super_user?

      ds = DataSink.new(id: 99999, org_id: (params[:org_id].to_i || current_org&.id))
      render json: ds.script_sink_config(request), status: :ok
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      @data_sinks = current_user.data_sinks(current_org, { access_role: request_access_role, access_roles: @access_roles })
      @data_sinks = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataSink, params[:filters], @data_sinks, sort_opts: sort_opts).call
      @data_sinks = @data_sinks.page(@page).per_page(@per_page)
      set_link_header(@data_sinks)

      if @expand
        DataMap.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
        DataCredentials.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
      end

      load_tags(@access_roles)
      render :index
    end

    def update_runtime_status
      return head :forbidden unless current_user.infrastructure_or_super_user?

      sink = DataSink.find(params[:id])
      return head :forbidden unless current_user.super_user? || (sink.org_id == current_org&.id)

      sink.update_runtime_status(params[:status])
      head :ok
    end

    def test_config
      raise Api::V1::ApiError.new(:bad_request, "Input must be present") if request.raw_post.blank?
      input = (validate_body_json DataSinkConfigTest).symbolize_keys
      data_credentials = DataCredentials.find input[:data_credentials_id]
      authorize! :read, data_credentials

      result = ProbeService.new(data_credentials).test_sink_config(input[:sink_config], input[:input])
      render :json => result, :status => result[:status]
    end
  end
end
