unless ENV['DISABLE_PROMETHEUS'].truthy?
  require 'prometheus_exporter'
  require 'prometheus_exporter/client'
end

class PrometheusMetric
  include Singleton
  extend Memoist

  # We're currently only use one type of metric.
  # More about types: https://prometheus.io/docs/concepts/metric_types/
  AVAILABLE_TYPES = [:counter].freeze

  def self.observe(name, type: :counter, value: 1)
    raise ArgumentError.new("Wrong #{type} type") unless AVAILABLE_TYPES.include?(type)

    instance.public_send(type, name).observe(value) unless ENV['DISABLE_PROMETHEUS'].truthy?
  end

  memoize def counter(name)
    prometheus_client.register(:counter, name.to_s, "#{name} metric")
  end

  memoize def prometheus_client
    PrometheusExporter::Client.default
  end
end
