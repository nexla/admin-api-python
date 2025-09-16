module Api::V1::DataSets
  class CatalogRefsController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      head :forbidden and return if !current_user.has_admin_access?(current_user)
      @data_sets_catalog_refs = add_request_filters(DataSetsCatalogRef, DataSetsCatalogRef).page(@page).per_page(@per_page)
      set_link_header(@data_sets_catalog_refs)
    end

    def show
      return if render_schema DataSetsCatalogRef
      @data_sets_catalog_refs = DataSetsCatalogRef.find(params[:id])
      authorize! :read, @data_sets_catalog_refs
    end

    def create
      input = (validate_body_json DataSetsCatalogRef).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_sets_catalog_refs = DataSetsCatalogRef.build_from_input(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json DataSetsCatalogRef).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_sets_catalog_refs = DataSetsCatalogRef.find(params[:id])
      authorize! :manage, @data_sets_catalog_refs
      @data_sets_catalog_refs.update_mutable!(api_user_info, input)
      render "show"
    end

    def bulk_update_refs
      input = JSON.parse(request.raw_post, symbolize_names: true)
      api_user_info = ApiUserInfo.new(current_user, current_org, input)

      @data_sets_catalog_refs = DataSetsCatalogRef.bulk_update_refs(api_user_info, input)
      render "index"
    end

    def destroy
      data_sets_catalog_ref = DataSetsCatalogRef.find(params[:id])
      authorize! :manage, data_sets_catalog_ref
      if params[:hard_delete]
        CatalogPluginsService.new.delete_ref(current_org, data_sets_catalog_ref.reference_id, data_sets_catalog_ref.catalog_config.data_credentials_id)
      end
      data_sets_catalog_ref.destroy
      head :ok
    end
  end
end
