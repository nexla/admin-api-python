module Api::V1  
  class CatalogConfigsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern

    skip_before_action :authenticate, only: [:mock_catalog_add]
    before_action :validate_api_user, except: [:mock_catalog_add, :index, :show]

    def validate_api_user
      # api-2.6.0, Allow catalog_config management only in Org contexts
      # and only by Org admins. No ACLs.
      raise Api::V1::ApiError.new(:method_not_allowed) if current_org.nil?
      raise Api::V1::ApiError.new(:forbidden) if !current_org.has_admin_access?(current_user)
    end

    def index
      @catalog_configs = CatalogConfig.where(:org => current_org).page(@page).per_page(@per_page)
      authorize! :read, current_org
      set_link_header(@catalog_configs)
    end

    def index_all
      raise Api::V1::ApiError.new(:forbidden) unless current_user.infrastructure_or_super_user?

      catalog_configs = add_request_filters(CatalogConfig.jit_preload, CatalogConfig)
      catalog_configs = catalog_configs.in_dataplane(request_dataplane) if request_dataplane.present?
      @catalog_configs = catalog_configs.page(@page).per_page(@per_page)

      set_link_header(@catalog_configs)
      render 'index'
    end
    
    def show
      return if render_schema CatalogConfig
      @catalog_config = CatalogConfig.find(params[:id])
      authorize! :read, @catalog_config
      # Note, no need to call authorize! here, only org
      # admins have access. See validate_api_user aove.
    end
    
    def create
      input = (validate_body_json CatalogConfig).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @catalog_config = CatalogConfig.build_from_input(api_user_info, input)
      render "show"
    end
    
    def update
      input = (validate_body_json CatalogConfig).symbolize_keys
      @catalog_config = CatalogConfig.find(params[:id])
      authorize! :manage, @catalog_config
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @catalog_config)
      @catalog_config.update_mutable!(api_user_info, input)
      render "show"
    end

    def start_bulk_create_update
      @catalog_config = CatalogConfig.find(params[:id])
      authorize! :manage, @catalog_config
      CatalogWorker::BulkCreateOrUpdate.perform_async @catalog_config.id
      head :ok
    end
    
    def destroy
      catalog_config = CatalogConfig.find(params[:id])
      authorize! :manage, catalog_config
      if catalog_config.status == "ACTIVE"
        raise Api::V1::ApiError.new(:method_not_allowed, "Cannot delete an active catalog config")
      end
      if params[:delete_catalog_entries].truthy?
        CatalogWorker::BulkDeleteEntities.perform_async catalog_config.id
      else
        catalog_config.destroy
      end
      head :ok
    end

    def check_job_status
      catalog_config = CatalogConfig.find(params[:id])
      authorize! :read, catalog_config
      if !catalog_config.job_id.nil?
        result = CatalogPluginsService.new.get_job_status(current_org, catalog_config.job_id)
      else
        result = {:output => "No job id found for catalog config #{catalog_config.id}"}
      end
      render :json => result
    end

    # REMOVE development/testing only
    def mock_catalog_add
      if (!params[:page].blank?)
        delay = params[:page].to_i
        delay = 0 if (delay > 300)
        sleep(delay) if (delay > 0)
      end
      render :json => {
        "pdsName" => "Monthly-3-9-8 US Stock OHLC Prices-562247613105-PDS",
        "s3FileSystem" => "Monthly-3-9-8 US Stock OHLC Prices-562247613105-S3FS",
        "s3Bucket" => "omniai-datasests-bala-test-bucket",
        "s3File" => "Monthly-3-9-8 US Stock OHLC Prices-562247613105-S3File-20210309162430535",
        "schemaName" => "Monthly-3-9-8 US Stock OHLC Prices-Schema",
        "pdsUID" => "774cb551-bc0f-436f-8c83-4a24f8b6f45e",
        "s3FileSystemUID" => "9892e3a2-3a0d-4102-9d30-69e9b8d20850",
        "s3BucketUID" => "6b0baec5-365c-49a1-92e6-0ed4dd7ce32a",
        "s3FileUID" => "01bcb640-8528-4c49-a009-491c72345255",
        "schemaUID" => "ebee9528-8b55-459e-a435-052e4dea3c37"
      }
    end

  end
end
