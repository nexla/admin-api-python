json.version data_set.get_latest_version
json.(data_set, :id, :owner_id, :org_id, :name, :description, :updated_at, :created_at, :status, :runtime_status)

if (@expand)
  json.(data_set, :source_schema_id, :source_schema)
  json.(data_set, :transform)
  json.output_schema data_set.output_schema_with_annotations
end