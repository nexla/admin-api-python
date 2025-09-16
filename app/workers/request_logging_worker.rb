class RequestLoggingWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'reporting'

  using Refinements::Median

  def perform(request_logging_entries)
    summarize_entries(request_logging_entries)
  rescue Exception => e
    info = { message: e.message }
    info[:trace] = e.backtrace.blank? ? [] :
      e.backtrace.select { |l| l.include?(Rails.root.to_s) }.map {|l| l.gsub(Rails.root.to_s, "")}
    Rails.configuration.x.response_time_logger.error(info.to_json)
  end

  def summarize_entries(entries)
    key = :latency if entries.first.key?(:latency)
    key ||= "latency"

    entries.sort! { |a, b| a[key] <=> b[key] }
    latencies = entries.pluck(key)

    s = {
      :count => entries.length,
      :max => latencies.last,
      :average => latencies.sum(0.0) / latencies.length,
      :median => latencies.median,
      :max_details => entries.last
    }

    Rails.configuration.x.response_time_logger.info(s.to_json)
  end
end
