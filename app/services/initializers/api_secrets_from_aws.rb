class Initializers::ApiSecretsFromAws < Initializers::ApiSecrets

  def self.assume_aws_role (secrets)
    secrets[:aws] ||= Hash.new
    secrets[:aws][:region] = ENV["AWS_REGION"]
    secrets[:aws][:region] = "us-east-1" if secrets[:aws][:region].blank?
    secrets[:aws][:web_identity_token_file] = ENV['AWS_WEB_IDENTITY_TOKEN_FILE']
    secrets[:aws][:role_arn] = ENV['AWS_ROLE_ARN']
    secrets[:aws][:http_proxy] = ENV['http_proxy']

    return false if secrets[:aws][:web_identity_token_file].blank?

    begin
      sts_opt = Hash.new
      sts_opt[:aws][:http_proxy] = secrets[:aws][:http_proxy] if !secrets[:aws][:http_proxy].blank?
      sts_client = Aws::STS::Client.new(sts_opt)
      secrets[:aws][:sts_role_credentials] = sts_client.assume_role_with_web_identity({
        duration_seconds: 3600, 
        role_arn: secrets[:aws][:role_arn], 
        role_session_name: "nexla-admin-api-knkhm0nw",
        web_identity_token: File.read(secrets[:aws][:web_identity_token_file])
      })
    rescue => e
      puts ">> AWS EXCEPTION: could not assume role with web identity: #{e.message}"
      return false
    end

    puts ">> Assumed AWS role"
    return true
  end

  def initialize
    raise "AWS Secrets Manager is unsupported"
  end
end
