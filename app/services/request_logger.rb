class RequestLogger
  include Singleton
  extend Memoist

  # How many request-logging entries to buffer before
  # triggering job to send to metrics service. 
  # Note: request logging is disabled in development mode
  # by default, but we set a smaller buffer size here for
  # easier debugging when logging is enabled.
  BUFFER_SIZE = Rails.env.development? ? 5 : 50

  def log(user, org, request, response)
    return unless enabled?

    mutex.synchronize do
      if buffer.size >= BUFFER_SIZE
        RequestLoggingWorker.perform_async(buffer.to_a)
        buffer.clear
      end
    end

    buffer << build_message(user, org, request, response)
    true
  end

  private

  def build_message(user, org, request, response)
    start_time = request.env['req_time'] || request.headers['req_time']
    {
      'user_id'    => user&.id,
      'org_id'     => org&.id,
      'host'       => request.headers['origin'].nil? ? request.host : request.headers['origin'],
      'user_agent' => request.user_agent.to_s.downcase,
      'status'     => response.response_code,
      'method'     => request.method,
      'path'       => request.fullpath,
      'latency'    => start_time ? ((Time.now.utc - start_time) * 1000).round : 0
    }
  end

  memoize def enabled?
    Rails.configuration.x.api["request_logging_enabled"]
  end

  memoize def buffer
    Concurrent::Array.new
  end

  memoize def mutex
    Mutex.new
  end
end