class Initializers::ApiSecretsFromFiles < Initializers::ApiSecrets
  if defined?(Instance_Secrets_Dir).nil?
    # Note, we check whether already defined or not because this
    # file gets loaded twice, once when lib/initializers/secrets.rb
    # is processed, and then when Rails loads app/* files.
    Instance_Secrets_Dir = (ENV["API_INSTANCE_SECRETS_DIR"] ||"config/instance")
    Database_Secrets_File = (ENV["API_DATABASE_SECRETS_FILE"] || "#{Instance_Secrets_Dir}/database-creds/database-creds")
    Redis_Secrets_Dir = "#{Instance_Secrets_Dir}/redis"
    Elasticsearch_Secrets_Dir = "#{Instance_Secrets_Dir}/elasticsearch"
    External_Services = "#{Instance_Secrets_Dir}/external"
    Server_Cert_File_Path = "config/instance/server/tls.crt"

    Backwards_Compatible_Secrets_File = "#{Instance_Secrets_Dir}/application-creds/application-creds"
  end

  def initialize
    @secrets = Hash.new
    @secrets[:manager] = Initializers::ApiSecrets::Managers[:files]
    load_database_secrets
    load_dataplane_secrets
    load_controlplane_secrets
    load_redis_secrets
    load_elasticsearch_secrets
    load_aws_role
    @secrets.freeze
  end

  protected

  def load_controlplane_secrets
    # Note, control-plane encryption keys are shared across all
    # dataplanes in a given installation. They can be read from 
    # any of the dataplane secrets files.
    dp_key = @secrets[:dataplanes].keys.last
    default_secrets = @secrets[:dataplanes][dp_key]

    @secrets[:dataplane_header] = (default_secrets[:dataplane_header] || "X-Nexla-Dataplane")

    @secrets[:enc] = Hash.new
    @secrets[:enc][:access_token_secret_key] = 
      RotatableKey.new(:access_token_secret_key, default_secrets)

    # These keys could be rotatable, but the way DataCredentials and
    # ApiAuthConfig encryption is currently implemented, they do not 
    # support trying decryption with both the current key and the old key,
    # if present.
    @secrets[:enc][:credentials_key] = default_secrets[:enc_key]
    @secrets[:enc][:auth_config_key] = default_secrets[:enc_auth_config]

    if ENV["API_USE_DATAPLANE_CERTS"].truthy?
      # See NEX-12031 and NEX-12032
      @secrets[:ssl_ca_file] = Server_Cert_File_Path
      puts ">> Using SSL cert file for infrastructure service requests"
    end

    puts ">> Loaded control plane secrets from: #{dp_key}"
  end

  def load_database_secrets
    f = JSON.parse!(File.read(Database_Secrets_File)).symbolize_keys
    @secrets[:database] = (Rails.env.development? || Rails.env.test?) ?
      f[Rails.env.to_sym].symbolize_keys : f

    # NEX-6845 removing support for setting db encoding
    # from environment or config. This was never handled
    # properly and only worked because we were reading
    # the wrong key, thus getting nil and defaulting to
    # mysql client default setting. Let's just use that
    # for now.
    @secrets[:database].delete(:encoding)
    puts ">> Loaded database secrets from #{Database_Secrets_File}"
  end

  def load_dataplane_secrets
    s = Hash.new
    Dir.foreach(Initializers::DataplaneServices::Dataplane_Dir) do |fn|
      next if [".", ".."].include?(fn)

      # Skip <dataplane>.json files...
      next if fn.include?(".")

      path = Initializers::DataplaneServices::Dataplane_Dir + "/" + fn
      old_format = false

      if File.directory?(path)
        old_format = true
        path += "/#{fn}"
      end

      begin
        ds = JSON.parse(File.read(path))
          .transform_keys { |k| self.transform_secrets_key(k) }
          .symbolize_keys
        if (old_format && !ds.key?(:dataplane_uid))
          load_backwards_compatible_dataplane_secrets(s, ds, fn, path)
        else
          load_multi_dataplane_secrets(s, ds, path)
        end
      rescue => e
        puts ">> Could not load dataplane secrets!: #{path}, #{e.message}"
      end
    end
    @secrets[:dataplanes] = s
  end

  def load_multi_dataplane_secrets (s, ds, path)
    uid = ds[:dataplane_uid]
    raise "Multi-dataplane secrets: #{path}, missing dataplane.uid key" if uid.blank?
    s[uid] = ds
    s[uid][:infrastructure_access_key] = RotatableKey.new(:api_access_key, s[uid])
    puts ">> Loaded multi-dataplane secrets: #{path} (#{uid})"
  end

  def load_backwards_compatible_dataplane_secrets (s, ds, filename, path)
    uid = filename.downcase
    load_application_creds if @secrets[:application_creds].blank?
    s[uid] = @secrets[:application_creds].deep_dup
    s[uid][:username] = ds[:username]
    s[uid][:password] = ds[:password]
    s[uid][:infrastructure_access_key] = RotatableKey.new(:api_access_key, s[uid])
    puts ">> Loaded backwards-compatible dataplane secrets: #{path} (#{uid})"
  end

  def load_application_creds
    # NOTE call this method ONLY when loading dataplane secrets files in
    # pre-multi-dataplane format (e.g. config/dataplane/<dataplane-name>/<dataplane-name>).
    # It is not necessary if the dataplane is spec'd in the multi-dataplane format 
    # (e.g. config/dataplane/<dataplane-name>)
    @secrets[:application_creds] = JSON.parse(File.read(Backwards_Compatible_Secrets_File))
      .transform_keys { |k| self.transform_secrets_key(k) }
      .symbolize_keys
  end

  def load_redis_secrets
    base_path = "#{Redis_Secrets_Dir}/%s"
    begin
      @secrets[:redis] = Hash.new
      ca_path = base_path % "ca.crt"
      @secrets[:redis][:redis_ca_file] = ca_path if File.exist?(ca_path)
      @secrets[:redis][:redis_password] = ENV["REDIS_CERT_PASSWORD"] if ENV["REDIS_CERT_PASSWORD"].present?
      tls_path = base_path % "tls.crt"
      @secrets[:redis][:redis_cert] = OpenSSL::X509::Certificate.new(File.read(tls_path)) if File.exist?(tls_path)
      key_path = base_path % "tls.key"
      @secrets[:redis][:redis_key] = OpenSSL::PKey::RSA.new(File.read(key_path)) if File.exist?(key_path)
      puts ">> Loaded Redis config from #{Redis_Secrets_Dir}"
    rescue => e
      puts ">> Error initializing Redis secrets from files: #{Redis_Secrets_Dir}, #{e.message}"
    end
  end

  def load_elasticsearch_secrets
    base_path = "#{Elasticsearch_Secrets_Dir}/%s"
    begin
      @secrets[:es] = Hash.new
      ca_path = base_path % "ca.crt"
      @secrets[:es][:es_ca_file] = ca_path if File.exist?(ca_path)
      tls_path = base_path % "tls.crt"
      @secrets[:es][:es_cert] = OpenSSL::X509::Certificate.new(File.read(tls_path)) if File.exist?(tls_path)
      key_path = base_path % "tls.key"
      @secrets[:es][:es_key] = OpenSSL::PKey::RSA.new(File.read(key_path)) if File.exist?(key_path)
      @secrets[:es][:es_username] = ENV["ELASTICSEARCH_USERNAME"] if ENV["ELASTICSEARCH_USERNAME"].present?
      @secrets[:es][:es_password] = ENV["ELASTICSEARCH_USER_PASSWORD"] if ENV["ELASTICSEARCH_USER_PASSWORD"].present?
      puts ">> Loaded Elastic Search secrets from #{Elasticsearch_Secrets_Dir}"    
    rescue => e
      puts ">> Error initializing Elastic Search config from files: #{Elasticsearch_Secrets_Dir}, #{e.message}"
    end
  end

  def load_aws_role
    # Note, currently this action is required in two code paths in Nexla
    # saas environments: 1) when using IAM authentication for the database
    # (disabled everywhere as of api-3.x), and 2) when handling POST/PUT
    # /aws_marketplace_token requests.
    if (@secrets[:database][:aws_iam_auth_enabled] || ENV["API_AWS_MARKETPLACE_ENABLED"].truthy? )
      Initializers::ApiSecretsFromAws.assume_aws_role(@secrets)
    end
  end

  def transform_secrets_key (key)
    {
      "api.enc.key" => "enc_key",
      "api.enc.auth_config" => "enc_auth_config",
      "api.enc.otp" => "enc_otp",
      "nexla.username" => "username",
      "nexla.password" => "password",
      "nexla.access_token_secret_key" => "access_token_secret_key",
      "nexla.access_token_secret_key_old" => "access_token_secret_key_old",
      "recaptcha.sitekey" => "recaptcha_sitekey",
      "recaptcha.secret" => "recaptcha_secretkey"
    }[key] || key.gsub(".", "_")
  end
end