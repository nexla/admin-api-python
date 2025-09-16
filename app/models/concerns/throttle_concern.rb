module ThrottleConcern
  extend ActiveSupport::Concern

  # Method that returns a `RateLimit` from model or parent
  # If not found the default limits are served
  # @param owned_limits [TrueClass, FalseClass] returns only current RateLimit without the chain
  # @returns RateLimit
  def rate_limit(owned_limits: false)
    if owned_limits
      RateLimit.find_by(id: rate_limit_id)
    else
      RateLimit.find_by(id: rate_limit_id) || rate_limit_parent&.rate_limit || RateLimit.default
    end
  end

  # Checks if user should be throttled based on setting set by DevOps
  def throttled?
    return true if rate_limit_parent&.throttled?
    return false unless throttle_until.present?
    throttle_until > Time.now
  end

  def identity
    "#{model_name.to_s.downcase}-#{self.id}"
  end

  def rate_limit_parent
    nil
  end
end
