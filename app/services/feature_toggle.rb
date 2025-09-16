# Module that helps what features are enabled
module FeatureToggle
  THRESHOLD = 1.minute.freeze
  AVAILABLE_TOGGLES = %i(
    test_experiment
    rate_limiting
    enforce_rate_limiting
    metrics_for_shared_dataset
    marketplace
    self_signup
    automatic_self_signup_approval
    metrics_origin_node_id_passthrough
    genai_code_safety
  ).freeze

  class << self
    # Checks if the feature is enabled via ENV variable
    # @param name Symbol name of the feature
    # @return <TrueClass,FalseClass>
    # @raise [ArgumentError] when passed non-existing feature name
    def enabled?(name)
      validate_argument!(name)

      redis_value = read_from_redis(name)
      if redis_value.nil?
        ENV['ENABLE_' + name.to_s.underscore.upcase].truthy?
      else
        redis_value
      end
    end

    # Removes all cached values from `Thread.current`
    # Usable in specs
    def reset_cache!
      Thread.current[:feature_toggles] = {}
    end

    # Enabled feature toggle in Redis
    # @param name Symbol name of the feature
    # @raise [ArgumentError] when passed non-existing feature name
    def enable!(name)
      validate_argument!(name)

      set_in_redis(name, true)
    end

    # Disables feature toggle in Redis
    # @param name Symbol name of the feature
    # @raise [ArgumentError] when passed non-existing feature name
    def disable!(name)
      validate_argument!(name)

      set_in_redis(name, false)
    end

    private

    def redis_service
      ::RedisService.new
    end

    def read_from_redis(name)
      Thread.current[:feature_toggles] ||= {}

      cached = Thread.current[:feature_toggles][name]
      if cached && cached[:until] > Time.now
        cached[:value]
      else
        value = redis_service.get(feature_redis_key(name))
        value = value.truthy? unless value.nil? # `nil` value should not be casted to `false`
        Thread.current[:feature_toggles][name] = { value: value, until: THRESHOLD.from_now }
        value
      end
    end

    def set_in_redis(name, value)
      redis_service.set_with_expire(feature_redis_key(name), value, 2.years)
      reset_cache!
    end

    def validate_argument!(name)
      raise ArgumentError unless AVAILABLE_TOGGLES.member?(name)
    end

    def feature_redis_key(name)
      "feature_toggle:#{Rails.env}:#{name}"
    end
  end
end
