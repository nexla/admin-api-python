class RedisServiceWorker
  include Sidekiq::Worker
  sidekiq_options queue: 'redis_service', retry: 5

  @@logger = Rails.configuration.x.redis_service_logger

  def perform (op)
    case op["op"]
    when "set_with_expire"
      if !op["set_by_seconds"].present? || (Time.now.utc.to_i > op["set_by_seconds"])
        op["message"] = "set_with_expire not completed in time (#{op['set_by_seconds']})"
        @@logger.info(op.to_json)
      else
        # Note, here we tell RedisService to raise errors so that
        # Sidekiq can put this job on the retry queue if necessary.
        RedisService.new(:raise_errors => true)
          .set_with_expire(op["key"], op["value"], op["seconds"], op["set_by_seconds"])
      end
    else
      op["message"] = "Invalid operation #{op['op']}, no retry"
      @@logger.info(op.to_json)
    end
  end
end
