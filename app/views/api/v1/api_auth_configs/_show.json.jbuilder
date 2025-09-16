json.(api_auth_config, :id)
json.partial! @api_root + "users/owner", user: api_auth_config.owner
json.partial! @api_root + "orgs/brief", org: api_auth_config.org

json.(api_auth_config,
  :uid,
  :protocol,
  :name,
  :description,
  :global,
  :auto_create_users_enabled,
  :name_identifier_format,
  :nexla_base_url
)

if api_auth_config.org.has_admin_access?(current_user)
  json.(api_auth_config, :secret_config)
end

if (api_auth_config.is_saml?)
  json.(api_auth_config,
    :service_entity_id,
    :assertion_consumer_url,
    :logout_url,
    :metadata_url,
    :idp_entity_id,
    :idp_sso_target_url,
    :idp_slo_target_url,
    :idp_cert,
    :security_settings
  )
elsif (api_auth_config.is_oidc?)
  json.(api_auth_config,
    :oidc_domain,
    :oidc_keys_url_key,
    :oidc_token_verify_url,
    :oidc_id_claims,
    :oidc_access_claims
  )
end

json.(api_auth_config, :client_config, :updated_at, :created_at)