require 'will_paginate/array'

module Api::V1
  class DataFlowsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    include Api::V1::ApiKeyAuth

    skip_before_action :authenticate, only: [:flow_metrics, :flow_logs]
    before_action only: [:flow_metrics, :flow_logs] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    before_action do
      @downstream_only = params[:downstream_only].truthy?
      @full_tree = params[:full_tree].truthy?
    end
    before_action :initialize_date_params, only: [:flow_metrics] 

    def owned_flows
      flows_params = {
        :user => current_user,
        :org => current_org,
        :data_source_where => {},
        :data_set_where => {}
      }

      if params.key?(:status)
        flows_params[:data_source_where][:status] = params[:status].upcase
        flows_params[:data_set_where][:status] = params[:status].upcase
      end

      flows_params[:data_source_where][:source_type] = params[:source_type] if params.key?(:source_type)
      return DataFlow.new(flows_params).flows_quick
    end

    def index
      if ([:owner, :owner_only].include?(request_access_role))
        index_owner
      elsif (current_org.has_admin_access?(current_user) && [:admin, :collaborator].include?(request_access_role))
        index_quick(:admin)
      elsif (current_org.has_collaborator_access?(current_user) && [:collaborator].include?(request_access_role))
        index_quick(:collaborator)
      else
        index_by_access_role
      end
    end

    def index_owner
      render json: owned_flows
    end

    def index_by_access_role
      # FN backwards-compatibility

      api_user_info = ApiUserInfo.new(current_user, current_org)

      # NOTE most_recent_limit is a workaround for environments where
      # the total flow count visible to the caller is in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on flows
      # list views.

      options = {
        access_role: request_access_role
      }

      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      options[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      origin_nodes = add_request_filters(
        current_user.origin_nodes(current_org, options), FlowNode
      )

      flows = []
      dfp = {
        :user => current_user,
        :org => current_org
      }

      origin_nodes.each do |fn|
        dfp[fn.resource_key] = fn.send(fn.resource_key)
        flows << DataFlow.new(dfp).flows(@downstream_only, @full_tree)
      end

      render json: DataFlow.merge_flows(flows)
    end

    def index_admin
      data_source_cnd = {}
      data_set_cnd = {}

      if params.key?(:status)
        data_source_cnd[:status] = params[:status].upcase
        data_set_cnd[:status] = params[:status].upcase
      end

      data_source_cnd[:source_type] = params[:source_type] if params.key?(:source_type)

      data_sources = current_user.data_sources(current_org, access_role: request_access_role)
        .where(data_source_cnd)
      data_sets = DataSet.derived_from_shared_or_public(nil, current_org)
      data_sets = data_sets.where(data_set_cnd) if !data_set_cnd.empty?

      flows_params = Hash.new
      flows_params[:data_sources] = data_sources.includes(
        :data_sink, :code_container, :data_credentials, 
        vendor_endpoint: [:vendor], data_sets: [:code_container, :external_sharers, 
        data_sinks: [data_credentials: [:org, :owner]]]
      )
      flows_params[:data_sets] = data_sets
      flows_params[:user] = current_user
      flows_params[:org] = current_org

      render json: DataFlow.new(flows_params).flows
    end

    def index_quick (access_role)
      flows_params = {
        :user => current_user,
        :org => current_org,
        :data_source_where => {},
        :data_set_where => {}
      }

      if params.key?(:status)
        flows_params[:data_source_where][:status] = params[:status].upcase
        flows_params[:data_set_where][:status] = params[:status].upcase
      end

      opts = { downstream_only: false, all: true, access_role: access_role }
      # NOTE most_recent_limit is a workaround for environments where
      # the total flow count visible to the caller is in the thousands.
      # See NEX-10613. This is happening for Clearwater Analytics in
      # particular, in their staging environment.
      #
      # REMOVE this workaround once UI supports pagination on flows
      # list views.
      most_recent_limit = ENV["FLOWS_LIMIT"].to_i
      opts[:most_recent_limit] = most_recent_limit if (most_recent_limit > 0)

      flows_params[:data_source_where][:source_type] = params[:source_type] if params.key?(:source_type)
      render json: DataFlow.new(flows_params).flows_quick(opts)
    end

    def show
      df = DataFlow.new(data_set_id: params[:id], user: current_user, org: current_org)
      authorize! :read, df
      render json: df.flows(@downstream_only, @full_tree)
    end

    def create
      head :method_not_allowed
    end

    def update
      head :method_not_allowed
    end

    def destroy
      head :method_not_allowed
    end
  end
end



