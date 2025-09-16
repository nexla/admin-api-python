json.partial! @api_root + "orgs/data_sets_catalog_configs/catalog_config_show", catalog_config: @catalog_config

if @data_sets.present?
  json.partial! @api_root + "orgs/data_sets_catalog_configs/data_sets_index", data_sets: @data_sets
else
  json.data_sets []
end