class RateLimit < ApplicationRecord
  extend Memoist

  # Limit of requests per user per second
  GEN_AI_LIMIT = ENV.fetch('GEN_AI_LIMIT', 30).to_i
  UI_LIMIT = ENV.fetch('UI_LIMIT', 30).to_i
  ADAPTIVE_FLOWS_RATE_LIMIT = ENV.fetch('ADAPTIVE_FLOWS_RATE_LIMIT', 100).to_i

  DAILY_MULTIPLIER = (60 * 60 * 6).freeze # Quarter of a day in seconds
  LIMITS = %w(
    common
    light
    medium
    high
  )

  class << self 
    extend Memoist

    memoize def default
      new({
        common: 10,
        light: 12,
        medium: 8,
        high: 5
      })
    end

    memoize def not_logged
      new({
        common: 2,
        light: 2,
        medium: 2,
        high: 1
      })
    end
  end

  has_many :users, dependent: :nullify
  has_many :orgs, dependent: :nullify

  def daily_multiplier
    DAILY_MULTIPLIER
  end

  def to_json(_=nil)
    LIMITS.map { |level| [level, public_send(level)] }.to_h
  end

  LIMITS.each do |level|
    define_method level do
      attributes[level].presence || default.attributes[level]
    end
  end

  private

  memoize def default
    self.class.default
  end
end
