class SystemLogger
  include Singleton
  extend Memoist

  # name => extend?
  AVAILABLE_LOGGERS = {
    rate_limiting: true,
    cron: false
  }

  def self.logger(*args)
    instance.logger(*args)
  end

  memoize def logger(name)
    extend = AVAILABLE_LOGGERS.fetch(name)

    instance = Logger.new(logger_for(name))
    if extend
      extend_logger(instance)
    else
      instance
    end
  end

  def ensure_log_dir!
    FileUtils.mkdir_p(log_dir)
  end

  private

  def logger_for(name)
    "#{log_dir}/#{name}.log"
  end

  def log_dir
    "#{Rails.root}/log"
  end

  def stdout_log_mod
    ActiveSupport::Logger.broadcast(Logger.new(STDOUT))
  end

  def extend_logger(logger)
    if !Rails.env.development? && !Rails.env.test?
      logger.extend(stdout_log_mod)
    end
    logger
  end
end

# Use when config/initializers/loggers.rb is deleted
# SystemLogger.instance.ensure_log_dir!
