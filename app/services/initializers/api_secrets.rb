module Initializers
  class ApiSecrets
    extend Forwardable

    if defined?(Initializers::ApiSecrets::Managers).nil?
      # Note, we check whether already defined or not because this
      # file gets loaded twice, once when lib/initializers/secrets.rb
      # is processed, and then when Rails loads app/* files.
      Managers = {
        files: "files",
        aws: "aws",
        google: "google",
        vault: "vault"
      }
    end

    def_delegators :@secrets, :[], :[]=, :keys, :dig

    def self.load (secrets_manager)
      sm = Initializers::ApiSecrets::Managers.key(secrets_manager.downcase)
      raise Api::V1::ApiError.new(:internal_server_error, "Unknown secrets manager") if sm.nil?

      case sm
      when :files
        puts ">> Using secrets manager: files"
        secrets = Initializers::ApiSecretsFromFiles.new
      else
        puts ">> Unsupported secrets manager!: #{sm}"
        secrets = nil
      end

      return secrets
    end

    def find_dataplane_secrets (key)
      return nil if key.blank? || !@secrets[:dataplanes].is_a?(Hash)
      s = @secrets[:dataplanes][key]
      return s if s.is_a?(Hash)

      @secrets[:dataplanes].keys.each do |k|
        dataplane_name = @secrets.dig(:dataplanes, k, :dataplane_name)&.downcase
        return @secrets[:dataplanes][k] if (dataplane_name == key.downcase)
      end
      nil
    end

    attr_reader :secrets
  end
end
