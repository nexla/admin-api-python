module Catalog
  module Actions
    class SyncDataSet

      extend Memoist

      def initialize(data_set)
        @data_set = data_set
      end

      def applicable?
        catalog_config.present?
      end

      def catalog_mode_auto?
        catalog_config.mode_auto?
      end

      def call
        raise Api::V1::ApiError.new("Org should have an active catalog config") unless catalog_config
        CatalogWorker::CreateOrUpdate.perform_async(@data_set.id, @data_set.org_id)
      end

      private
      attr_accessor :data_set

      memoize def catalog_config
        data_set.org&.catalog_configs&.find_by(status: "ACTIVE")
      end
    end
  end
end