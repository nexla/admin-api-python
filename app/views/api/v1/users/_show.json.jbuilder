json.(user, :id, :email, :full_name)
json.(user, :api_key) if defined?(show_api_key) && show_api_key

if current_org&.is_nexla_admin_org?
  if @org_member_roles.present?
    json.super_user !!@org_member_roles.dig(user.id, :super_user)
  else
    # Set user context before checking .super_user?
    user.org = current_org
    json.super_user user.super_user?
  end
end

json.impersonated user.impersonated?

if user.impersonated?
  json.impersonator do
    json.partial! @api_root + "users/impersonator", user: user.impersonator
  end
end

json.default_org do
  if (user.default_org.nil?)
    json.nil!
  else
    json.(user.default_org, :id, :name)
  end
end

if user.user_tier.present?
  json.user_tier do
    json.partial! @api_root + "user_tiers/show", user_tier: user.user_tier
  end
else
  json.user_tier nil
end

json.status user.account_status(current_org)
json.account_locked user.account_locked?

if @org_member_roles.present?
  json.org_memberships (@org_member_roles.dig(user.id, :memberships) || [])
else
  json.org_memberships do
    memberships = current_user.super_user? ? user.all_org_memberships : user.org_memberships
    json.partial! @api_root + "users/orgs", org_memberships: memberships
  end
end

json.custodian_for_orgs do
  json.array! user.custodian_for_orgs, :id, :name
end

json.custodian_for_domains do
  json.array! user.custodian_for_domains.map(&:id)
end

if (@expand)
  org = (user.id == current_user.id) ? current_org : user.default_org
  json.account_summary user.account_summary(:all, org) if org.present?
end

json.(user, :email_verified_at, :tos_signed_at, :updated_at, :created_at)
