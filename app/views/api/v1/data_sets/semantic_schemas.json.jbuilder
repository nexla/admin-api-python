  json.array! @semantic_schemas do |semantic_schema|
    json.(semantic_schema,
      :id,
      :owner_id,
      :org_id,
      :data_set_id,
      :schema,
      :created_at,
      :updated_at
    )
  end
