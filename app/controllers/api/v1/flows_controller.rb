require 'will_paginate/array'

module Api::V1
  class FlowsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern
    include Api::V1::ApiKeyAuth
    include ControlEventConcern

    # NOTE - api-3.1 will return all flows by
    # default. api-3.2 or 3.3 (maybe 3.5) will restore
    # enforced pagination. See NEX-9425.
    # Default_Per_Page = 20
    Default_Per_Page = PAGINATE_ALL_COUNT

    skip_before_action :authenticate, only: [:flow_metrics, :flow_logs, :run_profiles_activate, :run_status_for_flow]
    before_action only: [:flow_metrics, :flow_logs, :data_flows_metrics, :run_profiles_activate, :run_status_for_flow] do
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    before_action only: [:index, :show, :search] do
      @flows_only = params[:flows_only].truthy?
      @include_run_metrics = params[:include_run_metrics].truthy?
    end

    before_action do
      if action_name == 'index'
        @render_projects = current_user.super_user? && params[:org_id].present? ? false : true
      else
        @render_projects = true
      end
    end

    before_action only: [:daily, :total] do
      if params[:origin_node_id].present?
        flow_node = FlowNode.find(params[:origin_node_id])
        authorize! :read, flow_node
      end
    end

    before_action only: :active_flows_metrics do
      if params[:org_id].present?
        org = Org.find(params[:org_id])
        authorize! :read, org
      end

      if params[:owner_id].present?
        user = User.find(params[:owner_id])
        authorize! :read, user
      end
    end

    def flow_node_from_id_or_resource
      if params.key?(:resource_id) || params.key?(:key)
        id = (params[:resource_id] || params[params[:key]])
        flow_node = params[:model].find(id).flow_node
        raise Api::V1::ApiError.new(:method_not_allowed) if flow_node.nil?
      elsif params.key?(:id)
        flow_node = FlowNode.find(params[:id])
      else
        flow_node = FlowNode.find(params[:flow_id])
      end
      return flow_node
    end

    def index
      if !current_user.infrastructure_or_super_user? && params[:org_id].present?
        raise Api::V1::ApiError.new(:method_not_allowed)
      end

      # Here we override the default per_page of PAGINATE_ALL_COUNT
      # to limit flows list views to a more performant number and
      # force pagination to be enabled.
      page_size = params[:org_id].present? ? 100 : Default_Per_Page
      if params[:per_page].blank?
        # For metrics we enforce pagination (because Default_Per_Page is huge)
        # We shouldn't pass a lot of flows to metrics API.
        @per_page = @include_run_metrics ? 20 : page_size
      end
      @paginate = true
      @origin_only = params[:origin_only].truthy?

      api_user_info = ApiUserInfo.new(current_user, current_org)
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      # NOTE most_recent_limit is a workaround for environments where
      # the total flow count visible to the caller is in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on flows
      # list views.
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      fn = if current_user.super_user? && params[:org_id].present?
             FlowNode.where('flow_nodes.id = flow_nodes.origin_node_id')
                     .where('data_source_id IS NOT NULL OR shared_origin_node_id IS NOT NULL')
           else
             current_user.origin_nodes(current_org, options)
           end

      if @include_run_metrics
        fn = fn.joins(:data_source).where.not(data_sources: { last_run_id: nil }).reselect('flow_nodes.*')
        @per_page = 20 if @per_page.blank? #TODO: replace with Default_Per_Page once it's finally set.
        params[:sort_by] = 'run_id' if params[:sort_by].blank?
      end

      origin_nodes = add_request_filters(fn, FlowNode)
                       .page(@page)
                       .per_page(@per_page)
      sort_opts = params.slice(:sort_by, :sort_order)
      render_flows(api_user_info, origin_nodes, options, sort_opts)
    end

    def show
      show_provisioning and return if params[:provisioning].truthy?

      if params.key?(:model)
        flow_node = params[:model].find(params[params[:key]]).origin_node
      else
        flow_node = FlowNode.origin_node(params[:id])
      end
      head :not_found and return if flow_node.nil?
      authorize! :read, flow_node
      api_user_info = ApiUserInfo.new(current_user, current_org)
      render_flows(api_user_info, [flow_node], access_role: request_access_role)
    end


    def show_provisioning
      return head :forbidden if !current_user.infrastructure_or_super_user?

      if params.key?(:model)
        flow_node = params[:model].find(params[params[:key]]).flow_node
      else
        flow_node = FlowNode.find(params[:id])
      end

      return head :not_found if flow_node.nil?

      if !current_user.super_user?
        return head :forbidden if (current_org&.id != flow_node.org_id)
      end

      if flow_node.data_sink_id.blank? && flow_node.data_set_id.blank?
        raise Api::V1::ApiError.new(:bad_request, "Provisioning is only available for data_set and data_sink nodes.")
      end

      api_user_info = ApiUserInfo.new(current_user, current_org)
      builder = Flows::Builders::RenderBuilder.new(api_user_info, [flow_node])
      @resources = builder.build_provisioning(flow_node)

      render :show_provisioning
    end

    def update
      input = (validate_body_json FlowNode).symbolize_keys
      origin_node = flow_node_from_id_or_resource.origin_node
      authorize! :manage, origin_node
      api_user_info = ApiUserInfo.new(current_user, current_org, input, origin_node)
      origin_node.flow_update_mutable!(api_user_info, input, request)
      @flows = [origin_node.flow(api_user_info)]
      @resources = origin_node.resources(api_user_info)
      if (params[:backwards_compatible].truthy?)
        df = DataFlow.new(origin_node.resource_key => origin_node.resource.id,
          :user => current_user, :org => current_org)
        render json: df.flows
      else
        render_flows(api_user_info, [origin_node], {})
      end
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      origin_node = flow_node_from_id_or_resource.origin_node
      authorize! :manage, origin_node
      api_user_info = ApiUserInfo.new(current_user, current_org, input, origin_node)
      copy_origin_node = origin_node.flow_copy(api_user_info, input)

      if (params[:backwards_compatible].truthy?)
        df = DataFlow.new(copy_origin_node.resource_key => copy_origin_node.resource.id,
          :user => current_user, :org => current_org)
        render json: df.flows
      else
        render_flows(api_user_info, [copy_origin_node], {})
      end
    end

    def activate
      flow_node = flow_node_from_id_or_resource
      authorize! :operate, flow_node

      origin = flow_node.origin_node
      if origin.source_records_count_capped?
        raise Api::V1::ApiError.new(:bad_request, "Flow is rate limited")
      end

      if origin.data_source && !OrgTier.validate_data_source_activate(origin.data_source)
        raise Api::V1::ApiError.new(:bad_request, "Active data source limit would be exceeded.")
      end

      if params[:activate]
        flow_node.flow_activate!(all: (params[:all].truthy? || params[:full_tree].truthy?), run_now: params[:run_now])
      else
        all = params[:all].truthy? || params[:full_tree].truthy?
        return if process_async_request("BulkPauseFlows", {id: flow_node.id} )

        flow_node.flow_pause!(all: all)
      end

      api_user_info = ApiUserInfo.new(current_user, current_org, {}, flow_node.origin_node)
      if (params[:backwards_compatible].truthy?)
        df = DataFlow.new(flow_node.origin_node.resource_key => flow_node.origin_node.resource.id,
          :user => current_user, :org => current_org)
        render json: df.flows
      else
        render_flows(api_user_info, [flow_node.origin_node], {})
      end
    end

    def run_profiles_activate
      flow_node = flow_node_from_id_or_resource
      authorize! :operate, flow_node

      body = MultiJson.load(request.raw_post)
      api_user_info = ApiUserInfo.new(current_user, current_org)
      result = flow_node.run_profile_activate!(body, api_user_info)

      render json: result, status: result[:status]
    end

    def flow_accessors
      # This is a wrapper around the accessors() method defined
      # in AccessorsConcern. It makes sure we apply the accessors
      # request to the origin node of the flow.
      params[:flow_node_id] = flow_node_from_id_or_resource.origin_node_id
      params[:model] = FlowNode
      self.accessors
    end

    def flow_docs
      # This is a wrapper around the docs() method defined in
      # DocsConcern. It makes sure we apply the docs request
      # to the origin node of the flow.
      params[:flow_node_id] = flow_node_from_id_or_resource.origin_node_id
      params[:model] = FlowNode
      self.docs
    end

    def list_linked_flows
      @origin_node = FlowNode.origin_node(params[:flow_id])
      authorize! :read, @origin_node
      render "linked_flows"
    end

    def create_linked_flows
      @origin_node = manage_linked_flows(:reset)
      render "linked_flows"
    end

    def update_linked_flows
      @origin_node = manage_linked_flows(:add)
      render "linked_flows"
    end

    def delete_linked_flows
      @origin_node = manage_linked_flows(:remove)
      render "linked_flows"
    end

    def delete_all_linked_flows
      @origin_node = manage_linked_flows(:remove, true)
      render "linked_flows"
    end

    protected def manage_linked_flows (mode, all = false)
      origin_node = FlowNode.origin_node(params[:flow_id])
      authorize! :manage, origin_node

      if (mode == :remove) && (request.raw_post.empty? || all)
        mode = :reset
        input = { linked_flows: [] }
      else
        input = (validate_body_json LinkedFlowsList).symbolize_keys
      end

      api_user_info = ApiUserInfo.new(current_user, current_org, input, origin_node)
      origin_node.update_linked_flows(api_user_info, input, mode)
      origin_node
    end

    def destroy
      flow_node = flow_node_from_id_or_resource
      authorize! :manage, flow_node
      flow_node.flow_destroy(all: (params[:all].truthy? || params[:full_tree].truthy?))
      head :ok
    end

    def run_now
      flow_node = FlowNode.find(params[:flow_id]).origin_node
      authorize! :operate, flow_node

      data_source = flow_node.data_source

      unless data_source
        raise Api::V1::ApiError.new(:bad_request, "Flow is not based on a source")
      end

      unless flow_node.active? && data_source.active?
        raise Api::V1::ApiError.new(:bad_request, "Flow and data source should be active")
      end

      unless OrgTier.validate_data_source_activate(data_source)
        raise Api::V1::ApiError.new(:bad_request, "Active data source limit would be exceeded.")
      end

      ds_result = data_source.run_now!

      render :json => ds_result, :status => ds_result[:status]
    end

    def flow_metrics
      origin_node = flow_node_from_id_or_resource.origin_node
      parent_node = get_metrics_origin_node(origin_node)

      result = flow_metrics_by_node(parent_node)

      if origin_node != parent_node
        resource_ids(origin_node).each do |(model, ids)|
          array = result.dig(:metrics, 'data', model.to_s)

          next unless array.present?

          array.keep_if do |record|
            ids.include? record["id"]
          end
        end
      end

      render status: result[:status], json: result
    end

    def total
      pms = params.permit(:from, :to, :org_id, :owner_id, :origin_node_id)
      result = MetricsService.new.get_totals(current_org, pms, nil)

      render status: result[:status], json: result
    end

    def daily
      pms = params.permit(:from, :to, :org_id, :owner_id, :origin_node_id)
      result = MetricsService.new.get_daily(current_org, pms, nil)

      render status: result[:status], json: result
    end

    def active_flows_metrics
      pms = params.permit(:from, :to, :org_id, :owner_id, :orderBy, :sortorder, :page, :size, :aggregated, :sort_by, :sort_order, :per_page)
      pms[:org_id] ||= current_org.id

      if pms[:owner_id].blank? && request_access_role(:owner) == :owner
        pms[:owner_id] = current_user.id
      end

      pms[:orderby] = pms.delete(:sort_by) || pms.delete(:orderBy)
      pms[:sortorder] = pms.delete(:sort_order) if pms.key?(:sort_order)
      pms[:size] = pms.delete(:per_page) if pms.key?(:per_page)

      result = MetricsService.new.get_active_flows_metrics(current_org, pms, nil)
      render status: result[:status], json: result
    end

    def docs_recommendation
      flow_node = FlowNode.find(params[:flow_id]).origin_node
      authorize! :read, flow_node

      result = GenaiFusionService.new.flow_doc_recommendation(flow_node, current_org)
      render json: result, status: result[:status]
    end

    protected def get_metrics_origin_node(node)
      if node.shared_origin_node
        get_metrics_origin_node node.shared_origin_node
      else
        node
      end
    end

    protected def resource_ids(origin_node, origin_node_id = nil)
      api_user_info = ApiUserInfo.new(current_user, current_org, {}, origin_node)
      resources = origin_node.resources(api_user_info)
      {
        data_sources: resources[:data_sources].pluck(:id),
        data_sets: resources[:data_sets].pluck(:id),
        data_sinks: resources[:data_sinks].pluck(:id)
        # origin_node_ids: [origin_node_id].compact # Remove comment when removing feature flag
      }.tap do |parameters|
        parameters.merge!({origin_node_ids: [origin_node_id].compact}) if FeatureToggle.enabled?(:metrics_origin_node_id_passthrough)
      end
    end

    protected def flow_metrics_by_node(origin_node)
      authorize! :read, origin_node
      pms = params.permit(:from, :to, :page, :groupby, :runId, :per_page, :sortorder, :origin_node_id, :interval)
      ids = resource_ids(origin_node, pms[:origin_node_id])
      pms[:runId] = params[:run_id] if params.key?(:run_id)
      pms[:size] = params[:per_page]
      MetricsService.new.get_data_flow_metrics(current_org, ids, pms)
    end

    def flow_logs
      origin_node = flow_node_from_id_or_resource.origin_node
      authorize! :read, origin_node
      api_user_info = ApiUserInfo.new(current_user, current_org, {}, origin_node)
      resources = origin_node.resources(api_user_info)
      ids = {
        data_sources: resources[:data_sources].pluck(:id),
        data_sets: resources[:data_sets].pluck(:id),
        data_sinks: resources[:data_sinks].pluck(:id),
        run_id: params[:run_id].nil? ? -1 : params[:run_id]
      }

      # flow-execution service expects an integer run_id
      # We support the backwards-compatible runId, but
      # newer run_id takes precedence.
      run_id = params[:run_id]
      run_id ||= params[:runId]
      ids[:run_id] = run_id.nil? ? -1 : run_id.to_i

      # flow-execution service supports sort_order, order_by
      # and severity. Translate backwards compatible params
      # if they are present present and not overridden by the
      # newer params.
      sort_order = params.delete(:sortorder)
      params[:sort_order] = sort_order if params[:sort_order].blank?

      ids[:log_type] = params.delete(:log_type)

      order_by = params[:orderby] || params[:order_by]
      params.delete(:orderby)
      params.delete(:order_by)

      if order_by.present?
        params[:order_by] = order_by.underscore
        if !["ts"].include?(params[:order_by])
          raise Api::V1::ApiError.new(:bad_request,
            "Flow logs 'order_by' query parameter supports only the 'ts' option")
        end
      end

      severity = params.delete(:level)
      params[:severity] = severity if params[:severity].blank?

      pms = params.permit(
        :from, :to,
        :page, :consolidated,
        :severity,
        :order_by, :sort_order
      )
      pms[:size] = params[:per_page]

      if !pms[:from].blank?
        pms[:from] = DateInterval.unix_to_db_datetime_str(pms[:from]).gsub(" ", "T")
      end

      if !pms[:to].blank?
        pms[:to] = DateInterval.unix_to_db_datetime_str(pms[:to]).gsub(" ", "T")
      end

      result = FlowExecutionService.new
        .get_data_flow_logs(origin_node.org, ids, pms.to_hash.compact)
      render status: result[:status], json: result
    end

    def run_status
      flow_node = flow_node_from_id_or_resource
      authorize! :read, flow_node

      if !flow_node.resource.is_a?(DataSink)
        raise Api::V1::ApiError.new(:bad_request,
          "Run status request requires a data sink flow node")
      end

      api_user_info = ApiUserInfo.new(current_user, current_org, {}, flow_node.resource)
      result = flow_node.resource.run_status(api_user_info, params[:run_id])
      render status: result[:status], json: result
    end

    def run_status_for_flow
      flow_node = flow_node_from_id_or_resource
      authorize! :read, flow_node

      api_user_info = ApiUserInfo.new(current_user, current_org)
      data_sinks = flow_node.resources(api_user_info)[:data_sinks]

      statuses = data_sinks.map do |sink|
        # NOTE not necessary to authorize :read access to sink here,
        # it is granted automatically by :read access to the flow.
        begin
          sink.run_status(api_user_info, params[:run_id]).with_indifferent_access
        rescue Api::V1::ApiError => e
          { status: e.status.to_s, output: { sink_id: sink.id, error: e.message } }
        rescue => e
          { status: "error", output: { sink_id: sink.id, error: e.message } }
        end
      end

      render status: :ok, json: statuses
    rescue KeyError, NoMethodError
      render status: :ok, json: []
    end

    def search
      @per_page = Default_Per_Page if !params[:per_page].present?
      @paginate = true
      api_user_info = ApiUserInfo.new(current_user, current_org)
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      input = (MultiJson.load(request.raw_post) rescue {})
      input.symbolize_keys! if input.is_a?(Hash)

      filters = params[:filters].presence || input[:filters]
      sort_opts = params.slice(:sort_by, :sort_order).presence || input.slice(:sort_by, :sort_order)
      origin_nodes = ::Flows::Search::FlowSearchExecutor.new(current_user, current_org, filters, options[:access_role], nil, sort_opts).call
      origin_nodes = origin_nodes.paginate(page: @page, per_page: @per_page)

      sort_opts = params.slice(:sort_by, :sort_order)

      render_flows(api_user_info, origin_nodes, options, sort_opts)
    end

    def publish
      head :forbidden and return if !current_user.super_user?

      input = params.permit(%w[run_id resource_id resource_type log log_type severity event_time_millis]).to_h
      input.merge!({'org_id' => org.id})
      result = FlowExecutionService.new.publish_raw(current_org, input)
      render :json => result, :status => result[:status]
    end

    def create
      json ||= MultiJson.load(request.raw_post, symbolize_keys: true, mode: :compat)
      data_source_input = validate_body_json(DataSource, json[:data_source])
      sinks = Array.wrap(json[:data_sinks]).map { |data_sink_json| validate_body_json(DataSink, data_sink_json) } # Validate input

      api_user_info = ApiUserInfo.new(current_user, current_org, json)
      @data_source = DataSource.build_from_input(api_user_info, data_source_input, request)

      data_set_input = {data_source_id: @data_source.id}
      data_set = DataSet.build_from_input(api_user_info, data_set_input)

      sinks.each do |data_sink_input|
        DataSink.build_from_input(api_user_info, {data_set_id:data_set.id, **data_sink_input}, request)
      end

      api_user_info = ApiUserInfo.new(current_user, current_org)
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }
      render_flows(api_user_info, [@data_source.flow_node], options)
    end

    def insert_flow_node
      flow_node = FlowNode.find(params[:flow_id])
      authorize! :manage, flow_node

      input = (validate_body_json FlowNodeInsert).deep_symbolize_keys

      api_user_info = ApiUserInfo.new(current_user, current_org)
      flow_node = flow_node.insert_flow_node(api_user_info, input, params, request)
      render_flows(api_user_info, [flow_node.origin_node], access_role: request_access_role)
    end

    def remove_flow_node
      flow_node = FlowNode.find(params[:flow_id])
      authorize! :manage, flow_node

      input = request.raw_post.present? ? MultiJson.load(request.raw_post, symbolize_keys: true) : {}
      api_user_info = ApiUserInfo.new(current_user, current_org)
      origin = flow_node.origin_node

      flow_node.remove_flow_node(delete_resource: (input[:delete_resource].truthy? || params[:delete_resource].truthy?))
      render_flows(api_user_info, [origin], access_role: request_access_role)
    end

    def import
      template = if params.key?(:template_id)
        FlowTemplate.find(params[:template_id])
      elsif params.key?(:template_name)
        FlowTemplate.find_by(name: params[:template_name])
      elsif params.key?(:template_type)
        FlowTemplate.find_by(flow_type: params[:template_type], default: true)
      end

      raise Api::V1::ApiError.new(:not_found) if template.nil?

      api_user_info = ApiUserInfo.new(current_user, current_org)
      flow_node = Flows::Builders::TemplateBuilder.new(api_user_info, template).build

      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }
      render_flows(api_user_info, [flow_node], options)
    end

    def publish_rag
      flow_node = FlowNode.find(params[:flow_id])
      authorize! :manage, flow_node

      AiWebServiceCacheWorker.perform_async(flow_node.id)
      render json: { status: "ok" }
    end

    def update_samples
      flow_node = FlowNode.find(params[:flow_id])
      authorize! :manage, flow_node

      raise Api::V1::ApiError.new(:bad_request, "Flow is not api server") if flow_node.flow_type != FlowNode::Flow_Types[:api_server]

      input = MultiJson.load(request.raw_post)
      raise Api::V1::ApiError.new(:bad_request) unless input.is_a?(Hash)

      api_user_info = ApiUserInfo.new(current_user, current_org)
      result = DataSet.bulk_update_samples(flow_node.id, api_user_info, input, params[:replace_samples].truthy?)

      render json: result
    end

    protected

    def render_flows(api_user_info, flow_nodes, options = {}, sort_opts = {})
      render_result = Flows::Builders::RenderBuilder.new(api_user_info, flow_nodes.to_a, options, @flows_only, sort_opts).build
      @flows, @resources, @projects, @triggered_flows, @triggering_flows, @linked_flows =
        render_result.values_at(:flows, :resources, :projects, :triggered_flows, :triggering_flows, :linked_flows)

      if @include_run_metrics
        if @flows.blank?
          set_link_header(flow_nodes)
          return render :show
        end

        flows_ids_with_runs = @flows.map { |f| { origin_node_id: f[:node].id, run_id: f[:node].last_run_id } }
        org = (params[:org_id].present? ? Org.find(params[:org_id]) : current_org)

        metrics_result = MetricsService.new.runs_metrics(org, flows_ids_with_runs, params.to_unsafe_h)
        if metrics_result[:status] != 200
          render status: metrics_result[:status], json: metrics_result
          return
        end

        @dashboard_metrics = metrics_result[:metrics]
        @status = metrics_result[:status]
      end

      load_tags(@access_roles) unless @flows_only
      load_processing_status(@flows)
      set_link_header(flow_nodes)
      render :show
    end

    def load_processing_status(flows)
      origin_ids = flows.map{|f| f[:node].id }
      sources = DataSource.where(origin_node_id: origin_ids).pluck(:origin_node_id, :runtime_status)
      dsets = DataSet.where(origin_node_id: origin_ids).pluck(:origin_node_id, :runtime_status)
      sinks = DataSink.where(origin_node_id: origin_ids).pluck(:origin_node_id, :runtime_status)

      runtime_status_map = Hash.new { API_RUNTIME_STATUSES[:idle] }

      processing = API_RUNTIME_STATUSES[:processing]
      sources.each { |k, v| runtime_status_map[k] = processing if v == processing }
      dsets.each { |k, v| runtime_status_map[k] = processing if v == processing }
      sinks.each { |k, v| runtime_status_map[k] = processing if v == processing }

      flows.each do |flow|
        flow[:node]._runtime_status = runtime_status_map[flow[:node].id]
      end
    end
  end
end
