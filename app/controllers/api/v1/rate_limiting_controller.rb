module Api::V1
  class RateLimitingController < ApiController
    before_action except: :limits do
      raise Api::V1::ApiError.new(:unauthorized) unless current_user&.super_user?
    end

    def limits
      result = Hash.new { |h,k| h[k] = {} }

      if FeatureToggle.enabled?(:rate_limiting)
        rate_limit = current_user&.rate_limit || RateLimit.not_logged
        identity = current_user&.identity || request.ip.gsub('.', '_')

        Rack::Attack.configuration.throttles.each do |name, _throttle|
          weight, period = name.split('_').map(&:to_sym)
          next unless period

          time_period = period == :second ? 1.second : 1.day
          limit_multiplier = period == :second ? 1 : rate_limit.daily_multiplier

          result[period][weight] = {
            limit: rate_limit[weight].to_i * limit_multiplier,
            count: Rack::Attack.cache.get_count("#{name}:#{identity}", time_period).to_i
          }
        end
      end

      render json: result
    end

    def set_rate_limits
      rate_limit = resource.rate_limit(owned_limits: true) || RateLimit.new

      resource.transaction do
        rate_limit.update!(params.require(:rate_limit).permit(*RateLimit::LIMITS))
        resource.update!(rate_limit_id: rate_limit.id)
      end

      render json: {rate_limit: rate_limit.to_json, success: true}
    end

    def throttle
      resource.update(throttle_until: params[:throttle_until])

      render json: {throttled_until: resource.throttle_until, success: true}
    end

    private

    memoize def model
      model_class = params[:model]
      model_class = model_class.constantize if model_class.is_a?(String)
      model_class
    end

    def resource
      model.find(params[:id])
    end

    def authenticate
      super()
    rescue
      # Allow user to be unauthenticated, but try to authenticate
    end

    def validate_query_parameters
      # Do not validate as it uses StrongParameters
    end
  end
end
