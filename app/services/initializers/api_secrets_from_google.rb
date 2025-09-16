class Initializers::ApiSecretsFromGoogle < Initializers::ApiSecrets
  def initialize
    raise "Google Secrets Manager is unsupported"
  end
end
