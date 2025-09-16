json.(data_set, :id, :origin_node_id, :flow_node_id)
json.owner do
  json.(data_set.owner, :id, :full_name, :email, :email_verified_at)
end
json.partial! @api_root + "orgs/brief", org: data_set.org

json.(data_set, :version, :name, :description)
json.access_roles data_set.get_access_roles(current_user, current_org)

json.(data_set, :status, :data_credentials_id)
json.transform_id data_set.code_container_id
json.(data_set, :data_source_id)

if @expand
  json.partial! @api_root + "orgs/data_sets_catalog_configs/catalog_config_show", catalog_config: data_set.active_catalog_ref&.catalog_config
end

json.partial! @api_root + "orgs/data_sets_catalog_configs/catalog_ref_show", catalog_ref: data_set.active_catalog_ref, as: :catalog_ref

json.parent_data_sets do
  json.array! [data_set.parent_data_set].compact do |parent_data_set|
    json.(parent_data_set, :id, :owner_id, :org_id, :name, :description, :created_at, :updated_at)
    json.partial! @api_root + "orgs/data_sets_catalog_configs/catalog_ref_show", catalog_ref: parent_data_set.active_catalog_ref
    json.(parent_data_set, :data_source_id)

    json.data_source do
      if parent_data_set.data_source
        json.(parent_data_set.data_source, :id, :owner_id, :org_id)
      else
        {}
      end
    end

    json.(parent_data_set, :output_schema, :data_source_id)
  end
end

json.(data_set, :output_schema)
if @expand
  json.(data_set, :output_validation_schema, :output_validator_id, :output_schema_validation_enabled,
    :custom_config, :runtime_config, :data_sample_id, :copied_from_id)
end

json.(data_set, :semantic_schema_id)
if @expand
  json.semantic_schema data_set.semantic_schema&.schema
end

json.(data_set, :created_at, :updated_at)

json.tags data_set.tags_list
json.data_samples data_set.prepare_data_samples_with_metadata(data_set.data_samples)

json.docs do
  json.array! data_set.docs do |doc|
    json.(doc, :id, :name, :description, :doc_type, :public, :text, :created_at, :updated_at)
  end
end