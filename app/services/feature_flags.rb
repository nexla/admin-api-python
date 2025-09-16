module FeatureFlags

  CACHE_TTL = 1.minute
  FEATURE_FLAGS_URL = "https://cdn.growthbook.io/api/features".freeze

  class << self
    def feature_on?(feature, attributes = {}, api_user_info = nil)
      growthbook(attributes, api_user_info).on?(feature)
    end

    def feature_value(feature, default, attributes = {}, api_user_info = nil)
      growthbook(attributes, api_user_info).feature_value(feature, default)
    end

    def eval_feature(feature, attributes = {}, api_user_info = nil)
      growthbook(attributes, api_user_info).eval_feature(feature).value
    end

    private

    def growthbook(attributes = {}, api_user_info = nil)
      Thread.current[:growthbook] ||= Growthbook::Context.new(
        features: growthbook_features_json,
        attributes: attributes
      )

      if Thread.current[:growthbook].attributes != attributes
        Thread.current[:growthbook].attributes = attributes
      end

      unless Rails.env.production?
        Thread.current[:growthbook].on_feature_usage = GrowthbookFeatureUsageListener.new(
          user_id: api_user_info&.user&.id || 'system_user',
          org_id: api_user_info&.org&.id || 'system_org'
        )
      end

      Thread.current[:growthbook]
    end

    def growthbook_features_json
      GrowthbookCache.fetch("growthbook_features", expires_in: CACHE_TTL) do
        Rails.logger.info("Fetching GrowthBook features from the network")

        repo = Growthbook::FeatureRepository.new(
          endpoint: endpoint,
          decryption_key: nil
        )

        repo.fetch || {}
      end
    end

    def endpoint
      @endpoint ||= begin
        cluster_uid = Cluster.default_cluster.uid
        api_key = API_SECRETS[:dataplanes][cluster_uid][:growthbook_api_key] || ENV['GROWTHBOOK_API_KEY']
        Rails.logger.warn("Growthbook api key not present in environment") if api_key.blank?

        "#{FEATURE_FLAGS_URL}/#{api_key}"
      end
    end
  end
end
