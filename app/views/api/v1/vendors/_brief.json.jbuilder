json.(vendor, :id, :name, :display_name, :connection_type, :connector_id)

json.connector_name vendor.connector&.name

if !vendor.auth_templates.blank?
  json.auth_templates do
    json.array! vendor.auth_templates do |auth_template|
      json.(auth_template, :id, :name, :display_name)
    end
  end
else
  json.auth_templates []
end