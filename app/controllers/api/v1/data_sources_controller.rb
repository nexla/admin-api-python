module Api::V1
  class DataSourcesController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include ApiKeyConcern
    include DocsConcern
    include Api::V1::ApiKeyAuth
    include AccessorsConcern
    include ControlEventConcern

    skip_before_action :authenticate, only: [:metrics]
    before_action only: [:metrics] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    INDEX_ALL_MAX_PER_PAGE = Rails.env.test? ? 50 : 1000

    def load_run_ids
      return if !@access_roles[:data_sources].present?

      @run_ids = Hash.new
      @access_roles[:data_sources].keys.each do |data_source_id|
        @run_ids[data_source_id] = Array.new
      end
      s = [ :run_id, :data_source_id, :created_at ]
      DataSourcesRunId.where(data_source_id: @access_roles[:data_sources].keys).select(s).each do |run_id|
        if @run_ids[run_id.data_source_id].size < DataSource::Default_Run_Id_Count
          @run_ids[run_id.data_source_id] << run_id
        end
      end
    end

    def index
      options = {
        access_role: request_access_role,
        attributes: "*",
        access_roles: @access_roles
      }

      # NOTE most_recent_limit is a workaround for environments where
      # the total resource (data_source, data_sink, flow_nodes, etc) 
      # counts visible to the caller can be in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on data_sources
      # list views.
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      @data_sources = add_request_filters(
        current_user.data_sources(current_org, options), DataSource
      ).page(@page).per_page(@per_page)
      set_link_header(@data_sources)

      if @expand
        DataCredentials.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
        DataSink.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
      end

      load_tags(@access_roles)
    end

    def runs
      @data_source = DataSource.find(params[:id])
      authorize! :read, @data_source
      @run_ids = @data_source.run_ids.paginate(page: @page, per_page: @per_page)
      set_link_header(@run_ids)
    end

    def search
      options = {
        access_role: request_access_role,
        attributes: "*",
        access_roles: @access_roles
      }

      sort_opts = params.slice(:sort_by, :sort_order)

      @data_sources = current_user.data_sources(current_org, options)
      @data_sources = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataSource, params[:filters], @data_sources, sort_opts: sort_opts).call
      @data_sources = @data_sources.page(@page).per_page(@per_page)
      set_link_header(@data_sources)

      if @expand
        DataCredentials.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
        DataSink.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
      end

      load_tags(@access_roles)
      render "index"
    end

    def index_all
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      params[:org_id] = current_org&.id unless current_user.super_user?

      @data_sources = add_request_filters(
        DataSource.jit_preload,
        DataSource
      )

      @data_sources = @data_sources.in_dataplane(request_dataplane) if request_dataplane.present?
      @data_sources = @data_sources.page(@page).per_page(@per_page)
      set_link_header(@data_sources)
      render "optimized"
    end

    pagination :index_all, per_page: INDEX_ALL_MAX_PER_PAGE, enforce: false
    
    def index_all_condensed
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      @script_credentials = DataCredentials.find_by_id(DataSource::Script_Data_Credentials_Id)
      sort_opts = {}
      filter_opts = {}
      filter_opts["data_sources.status"] = params[:status].upcase if params[:status].is_a?(String)
      if params[:org_id].present?
        filter_opts["data_sources.org_id"] = params[:org_id].to_i
      elsif request_dataplane.present?
        filter_opts["data_sources.org_id"] = Org.in_dataplane(request_dataplane).pluck(:id)
      end
      resource_id = params[:resource_id].to_i
      filter_opts[:id] = resource_id.next..Float::INFINITY unless resource_id.zero?
      filter_opts[:org_id] = current_org&.id unless current_user.super_user?
      sort_opts[params[:sort_by].presence || :id] = params[:sort_order].presence || :asc if params[:sort_by].is_a?(String)

      @data_sources = DataSource.all_condensed(filter_opts, sort_opts)
        .page(@page).per_page(@per_page)
      load_triggers(@data_sources)
      set_link_header(@data_sources)
      render "condensed"
    end

    def index_all_ids
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      params[:org_id] = current_org&.id unless current_user.super_user?

      status_cnd = params[:status].split(",") if !params[:status].blank?
      cnd = {}
      cnd[:status] = status_cnd if status_cnd.present?
      cnd[:org_id] = params[:org_id] if !params[:org_id].blank?
      if params[:org_id].present?
        cnd[:org_id] = params[:org_id].to_i
      elsif request_dataplane.present?
        cnd[:org_id] = Org.in_dataplane(request_dataplane).pluck(:id)
      end
      if status_cnd.present? && (status_cnd.size > 1)
        ds = DataSource.where(cnd).pluck(:id, :status)
        resp = Hash.new
        ds.each do |d|
          resp[d[1]] ||= Array.new
          resp[d[1]] << d[0]
        end
        render json: resp
      else
        render json: DataSource.where(cnd).pluck(:id)
      end
    end

    def show
      return if render_schema DataSource
      @data_source = DataSource.find(params[:id])
      authorize! :read, @data_source
    end

    def create
      input = (validate_body_json DataSource).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_source = DataSource.build_from_input(api_user_info, input, request)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_data_source = DataSource.find(params[:data_source_id])
      authorize! :manage, copied_data_source
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_data_source)
      @data_source = copied_data_source.copy(api_user_info, input)
      render "show"
    end
    
    def update
      input = (validate_body_json DataSource).symbolize_keys
      @data_source = DataSource.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_source.nil?
      authorize! :manage, @data_source
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_source)
      @data_source.update_mutable!(api_user_info, input, request, params[:force].truthy?, run_now: params[:run_now])
      render "show"
    end
    
    def destroy
      data_source = DataSource.find(params[:id])
      authorize! :manage, data_source
      data_source.destroy
      head :ok
    end

    def activate
      @data_source = DataSource.find(params[:data_source_id])
      authorize! :operate, @data_source

      if @data_source.source_records_count_capped?
        raise Api::V1::ApiError.new(:bad_request, "Data source is rate limited")
      end

      if (params[:activate])
        @data_source.activate!(true, params[:force].truthy?, run_now: params[:run_now])
      else
        @data_source.pause!
      end
      render "show"
    end

    def run_now
      @data_source = DataSource.find(params[:data_source_id])
      authorize! :operate, @data_source

      if @data_source.source_records_count_capped?
        raise Api::V1::ApiError.new(:bad_request, "Data source is rate limited")
      end

      @data_source.run_now!

      render "show"
    end

    def ready
      @data_source = DataSource.find(params[:data_source_id])
      authorize! :operate, @data_source

      if @data_source.source_records_count_capped?
        raise Api::V1::ApiError.new(:bad_request, "Data source is rate limited")
      end

      @data_source.ready!

      render "show"
    end

    def data_sinks
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      head :internal_server_error and return if !data_source.origin_node.present?
      @data_sinks = data_source.origin_node.data_sinks.page(@page).per_page(@per_page)
      set_link_header(@data_sinks)
      render "api/v1/data_sinks/index"
    end

    def metrics
      input = request.raw_post.present? ? MultiJson.load(request.raw_post) : {}
      @data_source = DataSource.find(params[:data_source_id])
      authorize! :read, @data_source
      result = MetricsService.new.get_metric_data(@data_source, params.merge(input.slice('input')), request)
      render :json => result, :status => result[:status]
    end
    
    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_sources = ResourceTagging.search_by_tags(DataSource, input, current_user, request_access_role, current_org)
      set_link_header(@data_sources)
      render "index"
    end

    def get_offset
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      result = ControlService.new(data_source).get_offset(params[:data_set_id])
      render :json => result, :status => result[:status]
    end

    def get_quarantine_offset
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      result = ControlService.new(data_source).get_quarantine_offset(params[:data_set_id])
      render :json => result, :status => result[:status]
    end

    def reingest_files
      data_source = DataSource.find(params[:data_source_id])
      authorize! :operator, data_source
      result = ListingService.new.reingest_files(data_source, request.raw_post)
      data_source.reingest_status = result[:status].to_s
      data_source.reingest_at = Time.now
      data_source.save!
      render :json => result, :status => result[:status]
    end

    def get_reingested_files
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      result = ListingService.new.get_reingested_files(data_source)
      render :json => result, :status => result[:status]
    end

    def validate_config
      data_source = DataSource.find(params[:data_source_id])
      authorize! :read, data_source
      config = (!request.raw_post.blank?) ? request.raw_post : data_source.source_config
      result = ControlService.new(data_source).validate_config(config, data_source.connector.connection_type)
      render :json => result, :status => result[:status]
    end

    def update_runtime_status
      return head :forbidden unless current_user.infrastructure_or_super_user?

      source = DataSource.find(params[:id])
      return head :forbidden unless current_user.super_user? || (source.org_id == current_org&.id)

      source.update_runtime_status(params[:status])
      head :ok
    end

    def script_source_config
      # Informational endpoint to help verify data_source
      # script config for a given org/cluster without
      # having to create a new data_source.
      head :forbidden and return unless current_user.infrastructure_or_super_user?
      params[:org_id] = current_org&.id unless current_user.super_user?

      ds = DataSource.new(id: 99999, org_id: (params[:org_id].to_i || current_org&.id))
      render json: ds.script_source_config(request), status: :ok
    end

    def test_config
      raise Api::V1::ApiError.new(:bad_request, "Input must be present") if request.raw_post.blank?
      input = (validate_body_json DataSourceConfigTest).symbolize_keys

      data_credentials = DataCredentials.find input[:data_credentials_id]
      authorize! :read, data_credentials

      result = ProbeService.new(data_credentials).test_source_config(input[:source_config])
      render :json => result, :status => result[:status]
    end

    protected
    def load_triggers(data_sources)
      # unpaginated - use SQL to select triggers
      if @per_page == PAGINATE_ALL_COUNT || @per_page == 0
        @triggers = FlowTrigger.joins(:triggering_flow_node)
                               .all
                               .group_by(&:triggered_origin_node_id)
      else
        # paginated - use IDs to select triggers
        origin_ids = data_sources.map(&:origin_node_id)
        @triggers = FlowTrigger.where(triggered_origin_node_id: origin_ids)
                               .joins(:triggering_flow_node)
                               .group_by(&:triggered_origin_node_id)
      end
    end

  end
end


