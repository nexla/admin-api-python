require 'will_paginate/array'

module Api::V1::DataFlows
  class DataSinkController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    before_action :initialize_date_params, only: [:flow_metrics] 
    include Api::V1::ApiKeyAuth

    skip_before_action :authenticate, only: [:flow_metrics, :flow_logs]
    before_action only: [:flow_metrics, :flow_logs] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    before_action do
      @downstream_only = params[:downstream_only].truthy?
      @provisioning = params[:provisioning].truthy?
      @full_tree = params[:full_tree].truthy?
    end
      
    def index
      data_sinks = current_user.data_sinks(current_org, access_role: request_access_role).page(@page).per_page(@per_page)
      set_link_header(data_sinks)
      render json: DataFlow.new(data_sinks: data_sinks, user: current_user, org: current_org).flows(@downstream_only, @full_tree)
    end

    def show
      head :forbidden and return if (@provisioning && !current_user.infrastructure_or_super_user?)
      ids = validate_query_id_list(params[:id])

      status = :ok
      if ids.size > 1 || @provisioning
        existing_ids = []
        ids.each do |id|
          sink = DataSink.find_by_id(id)
          if sink.nil?
            if ids.size == 1
              status = :not_found
              break
            end
            next
          end
          unless current_user.super_user?
            if (sink.org_id != current_org&.id)
              status = :forbidden
              break
            end
          end
          existing_ids << id
        end
        ids = existing_ids
      end

      head status and return if (status != :ok)

      dfs = []
      ids.each do |id|
        df = DataFlow.new(data_sink_id: id.strip, user: current_user, org: current_org)
        if (@provisioning)
          pf = df.provisioning_flow(Rails.configuration.x.provisioning_logger)
          dfs << pf if !pf.blank?
        else
          authorize! :read, df
          dfs << df.flows(@downstream_only, @full_tree)
        end
      end
      if (@provisioning)
        render status: status, json: dfs
      else
        render status: status, json: (dfs.size < 2) ? dfs[0] : dfs
      end
    end      

    def create
      head :method_not_allowed
    end

    def update
      input = (validate_body_json DataFlow).symbolize_keys
      df = DataFlow.new(data_sink_id: params[:id], user: current_user, org: current_org)
      authorize! :manage, df

      input[:downstream_only] = @downstream_only
      api_user_info = ApiUserInfo.new(current_user, current_org, input, df)
      df.update_mutable!(api_user_info, input, request)

      render json: df.flows
    end

    def destroy
      df = DataFlow.new(data_sink_id: params[:id], user: current_user, org: current_org)
      authorize! :manage, df
      df.destroy(true, params[:include_dependent_flows].truthy?)
      head :ok
    end
  end
end


