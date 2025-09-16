json.data_sets data_sets do |data_set|
  json.partial! @api_root + "data_sets/show_with_catalog_ref", data_set: data_set
end