json.array! @data_sets do |data_set|

  json.(data_set,
    :id,
    :data_source_id,
    :status,
    :data_credentials_id,
    :runtime_status,
    :updated_at,
    :created_at
  )

  json.set! :tag_list, []

  json.set! :owner do
  	json.set! :id, data_set.owner_id
  end

  json.set! :org do
  	json.set! :id, data_set.org_id
  end

  json.set! :parent_data_set_ids, [data_set.parent_data_set_id].compact
end
