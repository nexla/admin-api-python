module Initializers
  class DataplaneServices
    extend Forwardable

    if defined?(Initializers::DataplaneServices::Services).nil?
      # Note, we check whether already defined or not because this
      # file gets loaded twice, once when lib/initializers/secrets.rb
      # is processed, and then when Rails loads app/* files.
      Services = {
        :aws => "aws",
        :control => "control",
        :data_characteristic => "data_characteristic",
        :data_ingestion => "data_ingestion",
        :data_sync => "data_sync",
        :elasticsearch => "elasticsearch",
        :file_upload => "file_upload",
        :flows_dashboard => "flows_dashboard",
        :listing => "listing",
        :metrics => "metrics",
        :metrics_aggregation => "metrics_aggregation",
        :notification => "notification",
        :probe => "probe",
        :redis => "redis",
        :sample => "sample",
        :transform => "transform",
        :monitor => "monitor",
        :flow_execution => "flow_execution",
        :catalog_plugins => "catalog_plugins",
        :genai_fusion => "genai_fusion",
        :script => "script",
        :script_sink => "script_sink",
        :script_source => "script_source",
        :cubejs => "cubejs",
        :ai_web => "ai_web",
        :quarantine_aggregation => "quarantine_aggregation"
      }

      Dataplane_Dir = (ENV["API_DATAPLANE_DIR"] || "config/dataplane")
    end

    def_delegators :@services, :[], :[]=, :keys, :dig

    def initialize
      @services = Hash.new
      load_dataplane_service_files
    end

    def load_dataplane_service_files
      Dir.foreach(Initializers::DataplaneServices::Dataplane_Dir) do |fn|
        next if [".", ".."].include?(fn)
        next if !fn.include?(".json")
        dataplane_name = fn.gsub(".json", "")
        path = Initializers::DataplaneServices::Dataplane_Dir + "/" + fn
        old_format = false
        begin
          services_data = JSON.parse(File.read(path)).deep_symbolize_keys
          uid = services_data.dig(:dataplane, :uid)
          if uid.blank?
            puts ">> WARNRING: dataplane.uid missing in #{path}, using backwards-compatibility mode"
            uid = dataplane_name
            old_format = true
          end
          @services[uid] = Hash.new
          if old_format
            @services[uid][:dataplane] = { uid: uid, name: uid }
          else
            @services[uid][:dataplane] = services_data[:dataplane]
          end
          Services.each do |service, value|
            @services[uid][service] = services_data[service].is_a?(Hash) ?
              services_data[service] : Hash.new
          end
          puts ">> Loaded dataplane services: #{dataplane_name}, #{uid}"
        rescue => e
          puts ">> WARNING: could not load #{dataplane_name} services: #{e.message}"
        end
      end
    end

    def find_services (key)
      return nil if key.blank?
      return @services[key] if !@services[key].blank?
      @services.keys.each do |k|
        dataplane_name = @services.dig(k, :dataplane, :name)&.downcase
        return @services[k] if (dataplane_name == key.downcase)
      end
      nil
    end

    attr_reader :services
  end
end
