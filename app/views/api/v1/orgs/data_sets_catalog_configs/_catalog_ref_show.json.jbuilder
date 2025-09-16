json.catalog_ref do
  if catalog_ref
    json.(catalog_ref, :id, :catalog_config_id, :status, :reference_id, :link, :error_msg, :created_at, :updated_at )
  else
    {}
  end
end