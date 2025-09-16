# for some reason sidekiq fails to autoload classes automatically.

require "#{Rails.root}/lib/search_service/org_search"
require "#{Rails.root}/lib/search_service/public_search"

module IndexingWorker

  class IndexPublicResources
    include Sidekiq::Worker

    sidekiq_options queue: "indexing_all_data", retry: 3

    def perform(file_path, batch_size=500)
      begin
        log_file = File.open("#{file_path}/public_index.log", "ab+")
        log_file.puts "Public resources indexing started"
        SearchService::PublicSearch.new.index_all_data(batch_size: batch_size)

        log_file.puts "Public resources indexing completed"
      rescue StandardError => e
        log_file.puts "Public resources indexing exception: #{e.class} #{e.message} #{e.backtrace}"
      ensure
        log_file&.close
      end
    end
  end

  # Note that the workers here are configured to retry 3 times
  # 3 retries is ~2m32s https://github.com/mperham/sidekiq/wiki/Error-Handling#automatic-job-retry
  # which hopefully should be enough to work through any transicent elasticsearch errors
  class IndexAllResources
    include Sidekiq::Worker
  
    sidekiq_options queue: "indexing_all_data", retry: 3

    def perform (org_id, file_path, batch_size=500)
      log_file = nil
      mapping_file = nil
      begin
        log_file = File.open("#{file_path}/#{org_id}.log", "ab+")
        org_search = SearchService::OrgSearch.new(org_id)
        org_search.index_all_data(batch_size: batch_size)

        log_file.puts "Resource indexing completed"
        mapping_file = File.open("#{file_path}/#{org_id}_mapping.json","ab+")
        mapping_file.write org_search.get_mapping
      rescue => e
        log_file&.puts("Resource indexing exception: #{e.class} #{e.message} #{e.backtrace}")
        # important to rethrow so sidekiq will retry for us
        raise e
      ensure
        mapping_file&.close
        log_file&.close
      end
    end
  end

  class IndexResource
    include Sidekiq::Worker
  
    sidekiq_options queue: "indexing", retry: 3

    def perform (event, model, id, org_id)
      begin
        logger = Rails.configuration.x.indexing_service_logger
        org_search = SearchService::OrgSearch.new(org_id)
        public_search = SearchService::PublicSearch.new
        klass = model.constantize
        if event.to_sym == :destroy          
          org_search.delete_data klass,id
          public_search.delete_data klass,id
        else
          record = klass.find_by(id: id)
          if record
            org_search.index_data(klass, record)
            public_search.index_data(klass, record)
          end
        end
        logger.info({ 
          event: event,
          model: model,
          id: id,
          org_id: org_id,
          details: "Successfully #{event} the data in Index"
        }.to_json)
      rescue => e
        logger.info({ 
          event: event,
          model: model,
          id: id,
          org_id: org_id, 
          error: "Error while creating the index data #{e.class} #{e.message}"
        }.to_json)
        # important to rethrow so sidekiq will retry for us
        raise e
      end
    end
  end

  class CreateIndex
    include Sidekiq::Worker
  
    sidekiq_options queue: "create_index", retry: 3

    def perform (org_id)
      begin 
        logger = Rails.configuration.x.indexing_service_logger
        SearchService::OrgSearch.new(org_id).create_mapping
        SearchService::PublicSearch.new.create_mapping
        logger.info({
          event: "Index Creation",
          org_id: org_id,
          details: "Index Creation is success"
        }.to_json)
      rescue => e
        logger.info({ 
          event: "Index Creation",
          org_id: org_id,
          error: "Error while creating the index #{e.class} #{e.message}"
        }.to_json)
        # important to rethrow so sidekiq will retry for us
        raise e
      end
    end
  end
end
  