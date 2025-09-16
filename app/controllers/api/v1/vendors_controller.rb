module Api::V1
  class VendorsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern

    def index
      if params.key?(:vendor_name)
        return show
      end
      @vendor = add_request_filters(Vendor.jit_preload, Vendor).page(@page).per_page(@per_page)
      set_link_header(@vendor)
    end

    def show
      return if render_schema Vendor
      if params.key?(:vendor_name)
        @vendor = Vendor.find_by_name!(params[:vendor_name])
      else
        @vendor = Vendor.find(params[:id])
      end
      render "show"
    end

    def create
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json Vendor).deep_symbolize_keys
      @vendor = Vendor.build_from_input(input)
      render "show"
    end

    def update
      head :forbidden and return if !current_user.super_user?

      input = (validate_body_json Vendor).deep_symbolize_keys
      if params.key?(:vendor_name)
        @vendor = Vendor.find_by_name!(params[:vendor_name])
      else
        @vendor = Vendor.find(params[:id])
      end

      @vendor.update_mutable!(input)
      render "show"
    end

    def destroy
      head :forbidden and return if !current_user.super_user?
      if params.key?(:vendor_name)
        vendor = Vendor.find_by_name!(params[:vendor_name])
      else
        vendor = Vendor.find(params[:id])
      end
      vendor.destroy
      head :ok
    end

    def destroy_auth_template
      head :forbidden and return if !current_user.super_user?

      vendor = Vendor.find(params[:vendor_id])
      template = vendor.auth_templates.find(params[:auth_template_id])
      template.destroy
      head :ok
    end
  end
end