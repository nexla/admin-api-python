json.array!(@resource.access_controls) do |ac|
  json.type ac.accessor_type
  json.id ac.accessor_id
  case ac.accessor_type
  when AccessControls::Accessor_Types[:user]
    json.email ac.accessor.email
  when AccessControls::Accessor_Types[:org]
    json.email_domain ac.accessor.email_domain
    json.client_identifier ac.accessor.client_identifier
  when AccessControls::Accessor_Types[:team]
    json.name ac.accessor.name
  end
  json.accessor_org_id ac.accessor_org_id
  json.access_roles ac.get_access_roles
  if ac.expires_at
    json.access_role_expires_at ac.expires_at
  end
  json.created_at ac.created_at
  json.updated_at ac.updated_at
end