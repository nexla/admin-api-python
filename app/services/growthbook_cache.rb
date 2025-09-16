module GrowthbookCache
  class << self
    def instance
      @instance ||= ActiveSupport::Cache::MemoryStore.new(size: 4.megabytes)
    end

    def fetch(key, options = {}, &)
      instance.fetch(key, options, &)
    end

    def write(key, value, options = {})
      instance.write(key, value, options)
    end

    def read(key, options = {})
      instance.read(key, options)
    end

    def delete(key, options = {})
      instance.delete(key, options)
    end

    def clear
      instance.clear
    end
  end
end
