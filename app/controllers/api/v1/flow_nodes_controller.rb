require 'will_paginate/array'

module Api::V1
  class FlowNodesController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    include Api::V1::ApiKeyAuth
    include AccessorsConcern

    def show
      @flow_node = FlowNode.find(params[:id])
      authorize! :read, @flow_node
    end

    def update
      input = (validate_body_json FlowNode).symbolize_keys
      @flow_node = FlowNode.find(params[:id])
      authorize! :manage, @flow_node
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @flow_node)
      @flow_node.update_mutable!(api_user_info, input, request)
      render "show"
    end
 
    def origin_nodes_condensed
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      sort_opts = {}
      filter_opts = {}

      if params[:org_id].present?
        filter_opts["org_id"] = params[:org_id].to_i
      elsif request_dataplane.present?
        filter_opts["org_id"] = Org.in_dataplane(request_dataplane).pluck(:id)
      end

      filter_opts["status"] = params[:status].upcase if params[:status].is_a?(String)
      filter_opts["ingestion_mode"] = params[:ingestion_mode].downcase if params[:ingestion_mode].is_a?(String)
      filter_opts["flow_type"] = params[:flow_type].downcase if params[:flow_type].is_a?(String)

      resource_id = params[:resource_id].to_i
      filter_opts[:id] = resource_id.next..Float::INFINITY unless resource_id.zero?
      sort_opts[params[:sort_by].presence || :id] = params[:sort_order].presence || :asc if params[:sort_by].is_a?(String)

      nodes = FlowNode.condensed_origins.where(filter_opts).order(sort_opts)
      @flow_nodes = nodes.page(@page).per_page(@per_page)

      set_link_header(@flow_nodes)
      render "condensed"
    end
  end
end

