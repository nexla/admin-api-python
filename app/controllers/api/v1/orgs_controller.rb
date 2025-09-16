require 'will_paginate/array'

module Api::V1  
  class OrgsController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include ControlEventConcern

    before_action only: [:audit_log, :resource_audit_log] do 
      initialize_date_params(:current_and_previous_q)
    end

    before_action do
      # Note, /orgs endpoints do not support the @expand=1
      # query parameter. It is safer to disable it here, in
      # case the caller passes it (some do) because it can
      # have unintended side-effects when orgs renderers 
      # call non-orgs renders (e.g. users show for org owner)
      @expand = false
      @admin_access = can?(:manage, Org)
    end

    def index
      resource_select = current_user.orgs(request_access_role(:all))
      @orgs = add_request_filters(resource_select, Org).page(@page).per_page(@per_page)
      @index_action = true
      set_link_header(@orgs)
    end

    def index_all
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      orgs = add_request_filters(Org, Org).page(@page).per_page(@per_page)
      orgs = orgs.in_dataplane(request_dataplane) if request_dataplane.present?
      @orgs = orgs.page(@page).per_page(@per_page)
      @index_action = true
      set_link_header(@orgs)

      render "index"
    end
    
    def show
      return if render_schema Org
      @org = Org.find(params[:id])
      authorize! :read, @org
    end
    
    def create
      head :forbidden and return if !current_user.super_user?
      input = (validate_body_json Org).symbolize_keys
      @org = Org.build_from_input(input, current_user, current_org)
      render "show"
    end

    def login_history
      @org = Org.find(params[:org_id])
      head :forbidden and return unless current_user.super_user? || current_org.has_admin_access?(current_user)
      @login_history = @org.login_audits.page(@page).per_page(@per_page) if params[:event_type].blank? || params[:event_type] == "login_history"
      @logout_history = @org.logout_audits.page(@page).per_page(@per_page) if params[:event_type].blank? || params[:event_type] == "logout_history"
    end
    
    def update
      input = (validate_body_json Org).symbolize_keys
      @org = Org.find(params[:id])
      authorize! :manage, @org
      if input.key?(:action)
        if input[:action].to_s.downcase == "source_activate"
          Org.activate_rate_limited_sources!(@org, @org.org_tier, true)
        else
          Org.pause_rate_limited_sources!(@org, @org.org_tier, input[:status],  0, true)
        end
      else
        @org.update_mutable!(request, current_user, current_org, input)
      end
      render "show"
    end

    def custodians
      @org = Org.find(params[:org_id])
      mode = params[:mode].to_sym
      if mode == :list
        authorize! :read, @org
        @custodians = @org.org_custodian_users
      else
        authorize! :manage, OrgCustodian
        input = validate_body_json(CustodiansRequest).symbolize_keys
        @custodians = @org.update_custodians!(current_user, input[:custodians], mode)
      end
      set_link_header(@custodians)
    end

    def update_cluster
      head :forbidden and return if !current_user.super_user?
      @org = Org.find(params[:org_id])
      input = validate_body_json({
        :type => :object,
        :additionalProperties => false,
        :properties => {
          :cluster_id => { :ref => :resource_id }
        }
      }).symbolize_keys
      @org.update_cluster(input[:cluster_id])
      render "show"
    end

    def revert_cluster
      head :forbidden and return if !current_user.super_user?
      @org = Org.find(params[:org_id])
      @org.revert_cluster
      render "show"
    end

    def set_cluster_status_active
      head :forbidden and return if !current_user.super_user?
      @org = Org.find(params[:org_id])
      @org.set_cluster_status_active
      render "show"
    end

    def destroy
      head :method_not_allowed
    end

    def activate
      head :forbidden and return if !current_user.super_user?

      @org = Org.find(params[:org_id])
      head :method_not_allowed and return if @org.is_nexla_admin_org?

      params[:activate] ? @org.activate! : @org.deactivate!
      render "show"
    end

    def metrics
      @org = Org.find(params[:org_id])
      authorize! :read, @org
      params[:metrics_name] ||= "org_owner_aggregate"
      result = MetricsService.new.get_metric_data(@org, params)
      render :json => result, :status => result[:status]
    end

    def audit_log
      return if process_async_request("GetAuditLogs", params)

      @org = Org.find(params[:org_id])
      authorize! :read, @org
      @date_interval = DateInterval.new("last_day") if @date_interval.is_blank?
      @audit_entries = AuditEntry.new(@org).all(@date_interval).paginate(:page => @page, :per_page => @per_page)
      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    def resource_audit_log
      return if process_async_request("GetAuditLogs", params)

      @org = Org.find(params[:org_id])
      authorize! :read, @org
      if !ConstantResolver.instance.versioned_models.include?(params[:resource_type].to_sym)
        raise Api::V1::ApiError.new(:bad_request)
      end
      @audit_entries = AuditEntry.new(@org).log_for_resource(params[:resource_type], @date_interval)
      event_filter, negate = AuditEntryFilterValidator.validate_event_filter(params)
      if event_filter.present?
        @audit_entries = @audit_entries.select { |v|
          has = (v.event == event_filter)
          (negate ? !has : has)
        }
      end
      @audit_entries = @audit_entries.paginate(:page => @page, :per_page => @per_page)
      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    def activate_rate_limited_sources
      @org = Org.find(params[:org_id])
      authorize! :operate, @org
      params[:activate] ? Org.activate_rate_limited_sources!(@org, @org.org_tier, true) :
        Org.pause_rate_limited_sources!(@org, @org.org_tier, params[:status],  0, true)
      render "show"
    end

    def account_rate_limited
      @org = Org.find(params[:org_id])
      authorize! :read, @org
      limited = false
      if @org.org_tier.present?
        limited = (@org.status == Org::Statuses[:source_data_capped])
      end
      render :json => { :org_rate_limited => limited }
    end

    def account_summary
      @org ||= Org.find(params[:org_id])

      unless @org.has_admin_access?(current_user)
        return head :forbidden
      end

      render json: AccountSummary::StatsService.new(@org).call
    end

    def current_org_account_summary
      @org = current_org
      account_summary
    end

    def flows_report
      org = Org.find(params[:org_id])
      head :forbidden and return if !org.has_admin_access?(current_user)
      if params[:by_destination].truthy?
        report = FlowsReport.generate_by_destination(org, status: DataSource::Statuses[:active])
      else
        report = FlowsReport.generate(org)
      end
      render status: :ok, json: report
    end
  end
end
