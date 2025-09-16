# Resolves problem with not restarting app after migration
# Allows us to alter constants after quick DB changes without restarting everything
class ConstantResolver
  include Singleton

  THRESHOLD = 10.minutes.freeze

  def self.constant_definition(name, use_ttl=true, &blk)
    define_method name do
      read_from_cache(name, use_ttl, &blk)
    end

    singleton_class.class_eval do
      define_method name do
        self.instance.public_send(name)
      end
    end
  end

  # call it via ConstantResolver.instance.nexset_api_connector_types
  # You can use `delegate :nexset_api_connector_types, to: :ConstantResolver` in Classes
  constant_definition(:nexset_api_connector_types) do
    Connector.nexset_api_compatible.pluck(:type) # || fallback_method?
  end

  constant_definition(:api_connector_types) do
    Connector.all_types_hash
  end

  constant_definition(:api_sink_types) do
    self.instance.api_connector_types
  end

  constant_definition(:api_source_types) do
    self.instance.api_connector_types.except(:data_map)
  end

  constant_definition(:versioned_models, false) do
    ActiveRecord::Base.connection.tables
                      .select { |t| t.end_with?("_versions") }
                      .select { |t| !t.include?("_depend") } # REMOVE after NEX-5834 is deployed
                      .select { |t| !t.include?("org_connection_config") } # REMOVE after NEX-5789 is deployed
                      .map { |t| t.gsub("_versions", "").to_sym }
  end

  constant_definition(:access_control_tables, false) do
    ActiveRecord::Base.connection.tables
                      .select { |t| t.end_with?("_access_controls") }
                      .select { |t| !t.include?("_subs") && !t.include?("_pubs") }
  end

  constant_definition(:access_control_models, false) do
    self.instance.access_control_tables.map do |t|
      begin
        t.singularize.camelcase.constantize
      rescue NameError => e
        puts "Warning: #{e.message} at #{e.backtrace ? e.backtrace[0] : __FILE__}"
        nil
      end
    end.compact
  end

  constant_definition(:access_controlled_models, false) do
    self.instance.access_control_tables.map do |t|
      begin
        if (t.include?("data_credentials") && t.exclude?("_groups"))
          t.gsub("_access_controls", "").camelcase.constantize
        else
          t.gsub("_access_controls", "").singularize.camelcase.constantize
        end
      rescue NameError => e
        puts "Warning: #{e.message} at #{e.backtrace ? e.backtrace[0] : __FILE__}"
        nil
      end
    end.compact
  end

  # Move it to some CacheService as it's 1:1 copy paste from FeatureToggle
  def read_from_cache(name, use_ttl=true)
    Thread.current[:constant_cache] ||= {}

    cached = Thread.current[:constant_cache][name]
    if cached
      if !use_ttl || (cached[:until] > Time.now)
        return cached[:value]
      end
    end

    value = yield.freeze

    value_hash = { value: value }
    if use_ttl
      value_hash[:until] = THRESHOLD.from_now
    end
    # Added `.present?` check to help with test env
    Thread.current[:constant_cache][name] = value_hash if value.present?
    value
  end
end
