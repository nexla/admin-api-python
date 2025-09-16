class ApiCronjob < ApplicationRecord
  class << self
    extend Memoist

    memoize def enabled?
      return false unless ActiveRecord::Base.connection.table_exists?(self.table_name)

      if ENV.key?('API_CRON_ENABLED')
        ENV['API_CRON_ENABLED'].truthy?
      else
        ["test", "development"].include?(Rails.env)
      end
    end

    memoize def lock_method
      v = ActiveRecord::Base.connection
        .select_values("select version()")
        .first.match('^(\d+).\d+.\d+')[1].to_i
      (v >= 8) ? :perform_lock_no_wait : :perform_lock
    end
  end

  def perform?
    self.reload
    perform = false
    return perform if (!ApiCronjob.enabled? || !self.is_due?)

    self.send(ApiCronjob.lock_method) do
      # Note, we have to check is_due? again, in case another
      # process ran the job after our first check above and
      # before we locked the row.
      if self.is_due?
        self.last_performed = @db_now
        self.save!
        true
      else
        false
      end
    end
  end

  def is_due?
    @db_now = ActiveRecord::Base.connection.select_values("select now()").first
    due = (self.last_performed.nil? || (@db_now > (self.last_performed + self.window_seconds)))
    return due
  end

  protected

  def perform_lock
    perform = false
    begin
      self.with_lock do
        perform = yield
      end
    rescue => e
      puts "CRON: Exception: #{e.inspect}"
    end

    return perform
  end

  def perform_lock_no_wait
    perform = false
    begin
      self.with_lock("FOR UPDATE NOWAIT") do
        perform = yield
      end
    rescue ActiveRecord::StatementInvalid => e
      # Ignore if record is already locked
    rescue => e
      puts "CRON: Exception: #{e.inspect}"
    end

    return perform
  end
end
