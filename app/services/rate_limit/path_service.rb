class RateLimit::PathService
  WEIGHTS = %i(common light medium high internal).freeze

  def initialize(path:, method:, request:)
    @path = path
    @method = method
    @request = request
    @hash = Rails.application.routes.recognize_path(path, method: method)
    @params = CGI::parse(path.split('?', 2)[1] || '')
    @controller = "#{hash[:controller]}_controller".camelize.constantize
  end

  # Determine weight of the path provided
  def weight
    return :interal if internal_request?
    return :common  if common_request?

    case method
    when "GET"
      determine_get_path
    when "POST", "PUT", "PATCH", "DELETE" then :medium
    end
  end

  WEIGHTS.each do |weight_name|
    define_method "#{weight_name}?" do
      default_limits? && weight_name == weight
    end
  end

  def adaptive_flows?
    request.url&.include?("/run_profiles/activate")
  end

  def gen_ai?
    user_agent == 'Nexla/GenAI'
  end

  def default_limits?
    [relaxed_limits?, gen_ai?].none?
  end

  # Determines if the request is likely from UI
  def relaxed_limits?
    looks_like_browser? && looks_like_real_request?
  end

  private

  def looks_like_browser?
    %w[Mozilla Chrom Safari IE].any? { |agent| user_agent&.include?(agent) }
  end

  def looks_like_real_request?
    %w[localhost nexla.com].any? { |host| request.referer&.include?(host) }
  end

  def determine_get_path
    case path
    when /data_set/ then :high
    when /users/ then extended? ? :high : :medium
    when /data_maps/ then :medium
    when /\/all($|\/)/ then :high
    else :light
    end
  end

  def common_request?
    controller == Api::V1::RateLimitingController
  end

  # Temporary solution
  def internal_request?
    action == 'raise_no_route!' || path =~ /^\/([\w_]+)?token/
  end

  def action
    hash[:action]
  end

  def extended?
    !!params['extended']&.first
  end

  attr_reader :path, :method, :hash, :controller, :params, :request
  delegate :user_agent, to: :request
end
