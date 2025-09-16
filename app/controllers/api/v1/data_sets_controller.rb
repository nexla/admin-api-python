module Api::V1  
  class DataSetsController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include TransformConcern
    include ApiKeyConcern
    include DocsConcern
    include AccessorsConcern
    include ControlEventConcern

    before_action only: [:index, :index_all, :show, :create] do
      @include_samples = params[:include_samples].truthy?
      @include_nexset_api_config = params[:include_nexset_api_config].truthy?
    end

    # This list contains the columns required
    # to render the index view. We use in select
    # statements to avoid fetching fields that
    # aren't used in that view, e.g. :data_samples,
    # which can be quite large.
    Index_Attributes = [
      :id,
      :origin_node_id,
      :flow_node_id, 
      :owner_id,
      :org_id,
      :name,
      :description,
      :status,
      :data_credentials_id,
      :runtime_status,
      :public,
      :managed,
      :data_source_id,
      :parent_data_set_id,
      :code_container_id,
      :output_schema,
      :output_schema_annotations,
      :copied_from_id,
      :created_at,
      :updated_at
    ]

    def index
      options = {
        access_role: request_access_role,
        attributes: (@expand.truthy? ? "*" : Index_Attributes),
        access_roles: @access_roles
      }

      # NOTE most_recent_limit is a workaround for environments where
      # the total resource (data_source, data_sink, flow_nodes, etc) 
      # counts visible to the caller can be in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on data_sets
      # list views.
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      @data_sets = add_request_filters(
        current_user.data_sets(current_org, options), DataSet
      ).page(@page).per_page(@per_page)
      set_link_header(@data_sets)

      if @expand
        DataSource.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
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

      # Note, this must be called after the @expand
      # block when all the related @access_roles
      # entries have been loaded...
      load_tags(@access_roles)
    end

    def index_all
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      head :method_not_allowed and return if !params[:data_source_id].present?

      # See NEX-10442 for details on the optimizations in this handler.
      # Some data_sources have hundreds or thousands of detected data_sets. 
      # That was causing so many db requests, and so much data to be transferred, 
      # that GET /data_sets/all?include_samples=1&data_source_id={id}&expand=1
      # was timing out for callers and tying up API processes for 60+ sec.

      @data_sets = DataSet.where(data_source_id: params[:data_source_id].to_i).jit_preload
      @data_sets = add_request_filters(@data_sets, DataSet)
        .page(@page).per_page(@per_page)

      # Arbitrary limit here. Without it, some requests return
      # 50 - 100mb responses due to thousands of data sets each have
      # large cached samples.
      @max_samples_count = (@data_sets.count > 100) ? 1 : DataSet::Max_Cached_Samples

      if !@data_sets.empty?
        # Similar to accessible_by_user() (see lib/accessible.rb), we load up
        # the access roles for all resources involved with a minimum of db requests
        # (none here, in fact, because the caller is always a Nexla admin).
        @access_roles[:data_sets] = Hash.new
        @data_sets.each do |ds|
          @access_roles[:data_sets][ds.id] = (current_user.id == ds.owner_id) ? :owner : :admin
        end
        @access_roles[:data_sources] = Hash.new
        @access_roles[:data_sources][@data_sets.first.data_source_id] = 
          (current_user.id == @data_sets.first.data_source.owner_id) ? :owner : :admin

        @sharers = Hash.new
        DataSetsAccessControl.where(role_index: AccessControls::ALL_ROLES_SET.index(:sharer),
          data_set_id: @data_sets.pluck(:id)).each do |ac|
          next if ac.data_set_id.nil?
          @sharers[ac.data_set_id] ||= Array.new
          @sharers[ac.data_set_id] << ac.render
        end

        load_tags(@access_roles)
      end

      set_link_header(@data_sets)
      render "index"
    end

    def index_all_condensed
      head :forbidden and return if !current_user.infrastructure_or_super_user?
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
      sort_opts[params[:sort_by].presence || :id] = params[:sort_order].presence || :asc if params[:sort_by].is_a?(String)

      @data_sets = DataSet.all_condensed(filter_opts, sort_opts)
      @data_sets = @data_sets.paginate(:page => @page, :per_page => @per_page)
      set_link_header(@data_sets)
      render "condensed"
    end

    def index_all_ids
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      cnd = {}
      cnd[:status] = params[:status] if !params[:status].blank?
      if params[:org_id].present?
        cnd[:org_id] = params[:org_id].to_i
      elsif request_dataplane.present?
        cnd[:org_id] = Org.in_dataplane(request_dataplane).pluck(:id)
      end
      render json: DataSet.where(cnd).select(:id).pluck(:id)
    end

    def public
      data_sets_query = DataSet.where(:public => true)
      @data_sets = add_request_filters(data_sets_query, DataSet).page(@page).per_page(@per_page)
      set_link_header(@data_sets)
      render "index"
    end

    def show
      return if render_schema DataSet
      @data_set = DataSet.find(params[:id])
      authorize! :read, @data_set
      @shared = @data_set.has_sharer_access_only?(current_user, current_org)
    end

    def summary
      @data_set = DataSet.find(params[:data_set_id])
      authorize! :read, @data_set
      render "summary"
    end

    def summaries
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @data_sets = add_request_filters(
        current_user.data_sets(current_org, options), DataSet
      ).page(@page).per_page(@per_page)

      load_tags(@access_roles)
      set_link_header(@data_sets)

      render "summaries"
    end
    
    def create
      input = (validate_body_json DataSet).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_set = DataSet.build_from_input(api_user_info, input, 
        params[:use_source_owner].truthy?, params[:detected].truthy?)

      catalog_sync = Catalog::Actions::SyncDataSet.new(@data_set)
      catalog_sync.call if catalog_sync.applicable? && catalog_sync.catalog_mode_auto?

      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_data_set = DataSet.find(params[:data_set_id])
      authorize! :manage, copied_data_set
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_data_set)

      if copied_data_set.parent_data_set&.splitter?
        raise Api::V1::ApiError.new(:bad_request, "Cannot clone a splitter child Nexset.")
      end
      @data_set = copied_data_set.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json DataSet).symbolize_keys

      @data_set = DataSet.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_set.nil?
      authorize! :manage, @data_set

      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_set)
      @data_set.update_mutable!(api_user_info, input, request)

      catalog_sync = Catalog::Actions::SyncDataSet.new(@data_set)
      catalog_sync.call if catalog_sync.applicable?
      render "show"
    end

    def destroy
      data_set = DataSet.find(params[:id])
      authorize! :manage, data_set
      data_set.destroy
      head :ok
    end

    def nexset_api_compatible
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      origin_nodes = current_user.origin_nodes(current_org, options)
        .where(nexset_api_compatible: true).jit_preload

      api_configs = Hash.new
      origin_nodes.each do |o|
        api_configs[o.id] = o.resource.nexset_api_config
      end

      status = (params[:status] || 'active' ).upcase
      cnd = { origin_node_id: origin_nodes.pluck(:id) }
      cnd = { status: status } if status != 'ALL'

      @data_sets = current_user.data_sets(current_org, options)
        .where(cnd)
        .with_api_keys
        .paginate(page: @page, per_page: @per_page)

      @data_sets.each do |ds|
        ds.nexset_api_config = api_configs[ds.origin_node_id]
      end

      load_tags(@access_roles)
      set_link_header(@data_sets)
      render "index_nexset_api"
    end

    def shared_with_user
      @data_sets = DataSet.shared_with_user(current_user, current_org).page(@page).per_page(@per_page)
      @shared = true
      set_link_header(@data_sets)
      render "index"
    end

    def search_shared_with_user
      scope = DataSet.shared_with_user(current_user, current_org)
      orgs = Org.where(id: scope.pluck('distinct org_id'))

      data_set_ids = orgs.flat_map do |org|
        Common::Search::BasicSearchExecutor.new(current_user, org, DataSet, params[:filters], scope).ids
      end.uniq

      @data_sets = scope.where(id: data_set_ids).page(@page).per_page(@per_page)
      @shared = true
      set_link_header(@data_sets)
      render "index"
    end

    def shared_by_user
      head :method_not_allowed
    end

    def semantic_schemas
      @data_set = DataSet.find(params[:data_set_id])
      authorize! :read, @data_set
      @semantic_schemas = @data_set.semantic_schemas.page(@page).per_page(@per_page)
      set_link_header(@semantic_schemas)
      render "semantic_schemas"
    end

    def samples
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set

      count = params[:count].to_i
      count = -1 if count <= 0

      # If this option isn't passed, default to true,
      # otherwise, use the input value.
      include_metadata = (!params.key?(:include_metadata) ||
        params[:include_metadata].truthy?)

      render :json => data_set.samples({ 
        :count => count,
        :output_only => params[:output_only].truthy?,
        :live => params[:live].truthy?,
        :include_metadata => include_metadata,
        from: params[:from]
      })
    end

    def update_samples
      input = MultiJson.load(request.raw_post)
      raise Api::V1::ApiError.new(:bad_request) if !input.is_a?(Array) && !input.is_a?(Hash)

      @data_set = DataSet.find_by_id(params[:data_set_id])
      raise Api::V1::ApiError.new(:not_found) if @data_set.nil?
      authorize! :manage, @data_set

      data_samples = input.is_a?(Array) ? input : [input]
      data_samples += (params[:replace] ? [] : @data_set.data_samples)

      @data_set.data_samples = data_samples[0...DataSet::Max_Cached_Samples]
      @data_set.save!

      render :json => @data_set.samples({
        :count => -1, :output_only => true
      })
    end

    def metrics
      @data_set = DataSet.find(params[:data_set_id])
      authorize! :read, @data_set
      result = MetricsService.new.get_metric_data(@data_set, params)
      render :json => result, :status => result[:status]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_sets = ResourceTagging.search_by_tags(DataSet, input, current_user, request_access_role, current_org, params[:public].truthy?)
      set_link_header(@data_sets)
      render "index"
    end

    def activate
      @data_set = DataSet.find(params[:data_set_id])
      authorize! :operate, @data_set
      params[:activate] ? @data_set.activate! : @data_set.pause!
      render "show"
    end

    def read_quarantine
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      input = MultiJson.load(request.raw_post)
      result = ProbeService.new(data_set).read_quarantine_sample(input.merge(:id => data_set.id))
      render :json => result, :status => result[:status]
    end

    def get_quarantine_offset
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      result = ControlService.new(data_set).get_quarantine_offset(data_set.id)
      render :json => result, :status => result[:status]
    end

    def get_offset
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      result = ControlService.new(data_set).get_offset(data_set.id)
      render :json => result, :status => result[:status]
    end

    def get_data_update_time
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      result = ControlService.new(data_set).get_data_update_time(data_set.id)
      render :json => result, :status => result[:status]
    end

    def get_characteristics
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      result = DataCharacteristicService.new.get_characteristics(data_set)
      render :json => result, :status => result[:status]
    end

    def characteristic_search
      org = current_org.nil? ? current_user.default_org : current_org

      result = DataCharacteristicService.new.search(current_user, org.id, params[:query])
      render :json => result, :status => result[:status]
    end

    def sync_with_catalog
      @data_set = DataSet.find_by_id(params[:data_set_id])
      raise Api::V1::ApiError.new(:not_found) if @data_set.nil?
      authorize! :manage, @data_set
      CatalogWorker::CreateOrUpdate.perform_async @data_set.id, @data_set.org_id
      head :ok
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      scope = current_user.data_sets(current_org, { access_role: request_access_role, access_roles: @access_roles })
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataSet, params[:filters], scope, sort_opts: sort_opts).call
      @data_sets = scope.page(@page).per_page(@per_page)
      set_link_header(@data_sets)

      if @expand
        DataSource.accessible_by_user(current_user, current_org,
          access_role: request_access_role,
          access_roles: @access_roles,
          access_roles_only: true
        )
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

    def update_runtime_status
      return head :forbidden unless current_user.infrastructure_or_super_user?

      data_set = DataSet.find(params[:id])

      data_set.update_runtime_status(params[:status])
      head :ok
    end

    def docs_recommendation
      data_set = DataSet.find(params[:data_set_id])
      authorize! :read, data_set
      result = GenaiFusionService.new.data_set_doc_recommendation(data_set, current_org)
      render json: result, status: result[:status]
    end

  end
end


