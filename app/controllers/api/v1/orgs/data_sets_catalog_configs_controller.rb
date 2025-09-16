module Api::V1::Orgs
  class DataSetsCatalogConfigsController < Api::V1::ApiController
    Default_Per_Page = 100

    def index
      org = Org.find(params[:id])
      authorize! :manage, org

      limit_pagination!
      @paginate = true

      @expand = false

      @catalog_config = CatalogConfig.find_by(org_id: org.id, status: "ACTIVE")
      if @catalog_config
        @data_sets = @catalog_config.data_sets.paginate(page: @page, per_page: @per_page)
      else
        @data_sets = DataSet.none.paginate(page: @page, per_page: @per_page)
      end
      set_link_header(@data_sets)
    end

    def show
      return head :forbidden unless current_user&.super_user?

      @data_set = DataSet.find_by(id: params[:id])
      return head :not_found unless @data_set

      authorize! :manage, @data_set.org

      @catalog_config = CatalogConfig.find_by(org_id: @data_set.org.id, status: "ACTIVE")
      @expand = true

      raise Api::V1::ApiError.new(:bad_request, "Org has no active catalog config") unless @catalog_config
    end

    protected
    def limit_pagination!
      @per_page = Default_Per_Page if params[:per_page].blank?
    end
  end
end
