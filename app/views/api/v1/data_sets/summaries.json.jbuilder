json.array!(@data_sets) do |data_set|
  json.(data_set, :id, :origin_node_id, :flow_node_id)
  summary = data_set.summary
  json.sharers summary[:sharers]
  json.data_sets summary[:data_sets]
  json.data_sinks summary[:data_sinks]
  if @tags[:data_sets].present?
    json.tags @tags[:data_sets][data_set.id]
  else
    json.tags data_set.tags_list
  end
end