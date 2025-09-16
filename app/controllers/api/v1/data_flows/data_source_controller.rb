require 'will_paginate/array'

module Api::V1::DataFlows
  class DataSourceController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    include Api::V1::ApiKeyAuth

    skip_before_action :authenticate, only: [:flow_metrics, :flow_logs]
    before_action only: [:flow_metrics, :flow_logs] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end
    before_action :initialize_date_params, only: [:flow_metrics] 

    def index
      data_sources = current_user.data_sources(request_access_role, current_org).page(@page).per_page(@per_page)
      set_link_header(data_sources)
      render json: DataFlow.new(data_sources: data_sources, user: current_user, org: current_org).flows
    end

    def show
      df = DataFlow.new(data_source_id: params[:id], user: current_user, org: current_org)
      authorize! :read, df
      render json: df.flows
    end      

    def create
      head :method_not_allowed
    end

    def update
      input = (validate_body_json DataFlow).symbolize_keys
      df = DataFlow.new(data_source_id: params[:id], user: current_user, org: current_org)
      authorize! :manage, df

      input[:downstream_only] = @downstream_only
      api_user_info = ApiUserInfo.new(current_user, current_org, input, df)
      df.update_mutable!(api_user_info, input, request)

      render json: df.flows
    end

    def destroy
      df = DataFlow.new(data_source_id: params[:id], user: current_user, org: current_org)
      authorize! :manage, df
      df.destroy(true, params[:include_dependent_flows].truthy?)
      head :ok
    end
  end
end


