module Api::V1  
  class ConnectorsController < Api::V1::ApiController
    def show
      no = Org.get_nexla_admin_org
      if (no.nil? || !current_user.org_member?(no))
        head :not_found and return
      end

      @connector = params[:id].to_i <= 0 ?
        Connector.find_by_type(params[:id]) : Connector.find(params[:id])

      head :not_found and return if @connector.nil?
    end

    def index
      no = Org.get_nexla_admin_org
      if (no.nil? || !current_user.org_member?(no))
        head :not_found and return
      end

      cnd = {}
      if (params.key?(:nexset_api_compatible) || params.key?(:sync_api_compatible))
        cnd[:nexset_api_compatible] = (params[:nexset_api_compatible] || 
          params[:sync_api_compatible]).truthy?
      end

      @connectors = Connector.where(cnd).page(@page).per_page(@per_page)
      set_link_header(@connectors)
    end

    def update
      head :forbidden and return unless current_user.super_user?

      @connector = Connector.find(params[:id])

      input = JSON.parse(request.raw_post).with_indifferent_access
      @connector.update_mutable!(input)
      render "show"
    end
  end
end