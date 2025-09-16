module Api::V1
  class VendorEndpointsController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      if params.key?(:vendor_endpoint_name)
        return show
      end
      @vendor_endpoints = add_request_filters(VendorEndpoint.jit_preload, VendorEndpoint).page(@page).per_page(@per_page)
      set_link_header(@vendor_endpoints)
    end

    def show
      return if render_schema VendorEndpoint
      if params.key?(:vendor_endpoint_name)
        @vendor_endpoint = VendorEndpoint.find_by_name!(params[:vendor_endpoint_name])
      else
        @vendor_endpoint = VendorEndpoint.find(params[:id])
      end
      render "show"
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json VendorEndpoint).symbolize_keys
      @vendor_endpoint = VendorEndpoint.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json VendorEndpoint).symbolize_keys
      if params.key?(:vendor_endpoint_name)
        @vendor_endpoint = VendorEndpoint.find_by_name!(params[:vendor_endpoint_name])
      else
        @vendor_endpoint = VendorEndpoint.find(params[:id])
      end

      @vendor_endpoint.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?

      if params.key?(:vendor_endpoint_name)
        vendor_endpoint = VendorEndpoint.find_by_name!(params[:vendor_endpoint_name])
      else
        vendor_endpoint = VendorEndpoint.find(params[:id])
      end
      vendor_endpoint.destroy
      head :ok
    end

  end
end