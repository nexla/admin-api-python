
json.org do
  if (org.nil?)
    json.nil!
  else
    json.(org,
      :id,
      :name,
      :cluster_id,
      :new_cluster_id,
      :cluster_status,
      :status,
      :email_domain,
      :email,
      :client_identifier,
      :self_signup,
      :features_enabled
    )

    if org.org_tier.present?
      json.org_tier do
        json.partial! @api_root + "org_tiers/show", org_tier: org.org_tier
      end
    else
      json.org_tier nil
    end

    if defined?(show_webhook_host) && show_webhook_host
      json.(org,
        :org_webhook_host,
        :nexset_api_host
      )
    end
  end
end
