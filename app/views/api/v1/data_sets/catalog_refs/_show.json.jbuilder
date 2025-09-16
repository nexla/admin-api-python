json.(data_sets_catalog_ref, :id)
json.partial! @api_root + "catalog_configs/show", catalog_config: data_sets_catalog_ref.catalog_config

json.(data_sets_catalog_ref,
  :id,
  :data_set_id,
  :status,
  :reference_id,
  :link,
  :error_msg,
  :updated_at,
  :created_at
)
