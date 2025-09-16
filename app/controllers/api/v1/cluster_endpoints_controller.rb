module Api::V1
  class ClusterEndpointsController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      head :forbidden and return if !current_user.super_user?
      @cluster_endpoint = ClusterEndpoint.all.page(@page).per_page(@per_page)
      set_link_header(@cluster_endpoint)
    end

    def show
      return if render_schema ClusterEndpoint

      @cluster_endpoint = ClusterEndpoint.find(params[:id])
      authorize! :read, @cluster_endpoint
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json ClusterEndpoint).symbolize_keys
      @cluster_endpoint = ClusterEndpoint.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json ClusterEndpoint).symbolize_keys
      @cluster_endpoint = ClusterEndpoint.find(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @cluster_endpoint.nil?

      @cluster_endpoint.update_mutable!(input)
      render "show"
    end

    def destroy
      head :method_not_allowed
    end

  end
end