module CatalogWorker

  class BulkCreateOrUpdate
    include Sidekiq::Worker

    sidekiq_options queue: 'cataloging', retry: 3

    def perform (catalog_config_id)
      # For all dataset in organization create data_sets_catalog_ref record.
      logger = Rails.configuration.x.catalog_service_logger
      catalog_config = CatalogConfig.find(catalog_config_id)
      logger.info("Get or Create data_sets_catalog_ref records for all datasets in org #{catalog_config.org_id}")

      if catalog_config.data_credentials&.connector_type != ConstantResolver.instance.api_connector_types[:data_world]
        # FIXME `find_by` method doesn't accept a block, so this logic is being ignored during execution
        DataSet.find_by(org_id: catalog_config.org_id) do |data_set|
          data_set_catalog_ref = DataSetsCatalogRef.find_or_initialize_by(data_set: data_set, catalog_config: catalog_config)
          data_set_catalog_ref.status = DataSetsCatalogRef::Statuses[:pending]
          data_set_catalog_ref.save!
        end
      end
      # Send bulk request to catalog service
      begin
        logger.info("Sending bulk request to catalog service for org #{catalog_config.org_id}")
        response = CatalogPluginsService.new.bulk_create_or_update(catalog_config.org)
        # Update job_id in catalog_config to save the job_id
        logger.info("Updating job_id in catalog_config for org #{catalog_config.org_id}")
        logger.info(response)
        catalog_config.job_id = response[:output]
        catalog_config.save!
      rescue => e
        logger.error("Error while bulk cataloging for org #{catalog_config.org_id} error: #{e}")
      end
    end
  end

  class CreateOrUpdate
    include Sidekiq::Worker

    sidekiq_options queue: 'cataloging', retry: 3

    def perform (data_set_id, org_id)
      # Create data_sets_catalog_ref record.
      logger = Rails.configuration.x.catalog_service_logger
      org = Org.find(org_id)
      catalog_config = org.active_catalog_config
      logger.info("Get or Create data_sets_catalog_ref records for dataset #{data_set_id} in org #{org_id}")

      unless catalog_config
        logger.warn("CatalogWorker::BulkDeleteEntities: no active catalog config for org #{org_id}")
        return
      end

      data_set = DataSet.find(data_set_id)
      data_set_catalog_ref = DataSetsCatalogRef.find_or_initialize_by(data_set_id: data_set_id, catalog_config: catalog_config)
      data_set_catalog_ref.status = DataSetsCatalogRef::Statuses[:pending]
      data_set_catalog_ref.save!
      begin
        # Send request to catalog service
        logger.info("Sending request to catalog service for org #{catalog_config.org_id}")
        response = CatalogPluginsService.new.create_or_update(org, data_set_catalog_ref.data_set_id)
      rescue => e
        logger.error("Error while cataloging for data_set #{data_set.id} in org #{catalog_config.org_id} error: #{e}")
        data_set_catalog_ref.status = DataSetsCatalogRef::Statuses[:error]
        data_set_catalog_ref.error_msg = e
        data_set_catalog_ref.save!
      end
    end
  end
  class BulkDeleteEntities
      include Sidekiq::Worker
  
      sidekiq_options queue: 'cataloging', retry: 3
  
      def perform (catalog_config_id)
        # Create data_sets_catalog_ref record.
        logger = Rails.configuration.x.catalog_service_logger
        catalog_config = CatalogConfig.find(catalog_config_id)
        unless catalog_config
          logger.warn("CatalogWorker::BulkDeleteEntities: no active catalog config for org #{org_id}")
          return
        end
        ref_ids = DataSetsCatalogRef.where(catalog_config_id: catalog_config_id).pluck(:reference_id)
        begin
          logger.info("Sending bulk request to catalog service for org #{catalog_config.org_id}")
          response = CatalogPluginsService.new.bulk_delete_refs(catalog_config.org, catalog_config.data_credentials_id, ref_ids)
          # Update job_id in catalog_config to save the job_id
          logger.info("bulk delete references for org #{catalog_config.org_id}")
          logger.info(response)
        rescue StandardError => e
          puts e.message
          puts e.backtrace
          logger.error("Error while bulk cataloging for org #{catalog_config.org_id} error: #{e}")
        end
      end
  end
end
