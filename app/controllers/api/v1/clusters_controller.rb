module Api::V1
  class ClustersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    before_action do
      raise Api::V1::ApiError.new(:forbidden) if !current_user.super_user?
    end

    def index
      @clusters = Cluster.page(@page).per_page(@per_page)
      set_link_header(@clusters)
      render "index"
    end

    def show
      return if render_schema Cluster
      @cluster = Cluster.find(params[:id])
      render "show"
    end

    def create
      input = (validate_body_json Cluster).deep_symbolize_keys
      endpoints = input[:endpoints]
      input.delete(:endpoints)
      valid_endpoints = []
      if endpoints.is_a?(Array)
        endpoints.each do |e|
          valid_endpoints << (validate_input_hash(ClusterEndpoint, e)).deep_symbolize_keys
        end
      end
      @cluster = Cluster.build_from_input(input, valid_endpoints)
      render "show"
    end

    def update
      input = (validate_body_json Cluster).deep_symbolize_keys
      endpoints = input[:endpoints]
      input.delete(:endpoints)

      @cluster = Cluster.find(params[:id])      
      valid_endpoints = []

      if endpoints.is_a?(Array)
        endpoints.each do |ep|
          existing_ep = @cluster.cluster_endpoints.find { |e| e.service == ep[:service] }
          rm = existing_ep.present? ? :put : :post
          valid_endpoints << (validate_input_hash(ClusterEndpoint, ep, rm)).deep_symbolize_keys
        end
      end
      @cluster.update_mutable!(input, endpoints)

      # Reload the cluster in case associated
      # endpoints where changed or created.
      @cluster.reload

      render "show"
    end

    def destroy
      cluster = Cluster.find(params[:id])
      cluster.destroy
      head :ok
    end

    def destroy_endpoint
      cluster = Cluster.find(params[:cluster_id])
      ep = cluster.cluster_endpoints.find(params[:endpoint_id])
      ep.destroy
      head :ok
    end

    def activate
      @cluster = Cluster.find(params[:cluster_id])
      @cluster.activate!
      render "show"
    end

    def set_default
      @cluster = Cluster.find(params[:cluster_id])
      @cluster.set_default
      render "show"
    end
  end
end