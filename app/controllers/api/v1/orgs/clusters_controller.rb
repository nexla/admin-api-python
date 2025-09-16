module Api::V1::Orgs
  class ClustersController < Api::V1::ApiController

    def index
      @org = Org.find(params[:org_id])
      authorize! :read, @org
      render "api/v1/orgs/clusters"
    end

  end
end
