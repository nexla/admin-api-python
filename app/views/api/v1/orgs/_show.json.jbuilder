json.(org, 
  :id,
  :name,
  :description,
  :cluster_id,
  :new_cluster_id,
  :cluster_status,
  :status,
  :email_domain,
  :email,
  :client_identifier,
  :self_signup,
  :org_webhook_host,
  :nexset_api_host
)

json.access_roles org.get_access_roles(current_user, current_org)

  if @admin_access
    json.owner do
      if @index_action
        json.(org.owner, :id, :full_name, :email)
      else
        json.partial! @api_root + "users/show", user: org.owner
      end
    end
    if (org.billing_owner.nil?)
      json.billing_owner nil
    else
      json.billing_owner do
        if @index_action
          json.(org.billing_owner, :id, :full_name, :email)
        else
          json.partial! @api_root + "users/show", user: org.billing_owner
        end
      end
    end
  end

unless @index_action
  json.admins(org.admin_users) do |user|
    json.(user, :id, :full_name, :email)
  end
end

if org.org_tier.present?
  json.org_tier do
    json.partial! @api_root + "org_tiers/show", org_tier: org.org_tier
  end
else
  json.org_tier nil
end

json.(org, :features_enabled)

json.(org, :require_org_admin_to_publish)
json.(org, :require_org_admin_to_subscribe)
json.(org, :email_domain_verified_at, :name_verified_at, :enable_nexla_password_login)
json.referenced_resources_enabled org.referenced_resources_enabled.truthy?
json.(org, :updated_at, :created_at)

if org.self_signup?
  json.(org, :trial_expires_at, :self_signup_members_limit)
end
