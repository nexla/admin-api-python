json.(@data_set, :id, :origin_node_id, :flow_node_id)
summary = @data_set.summary
json.sharers summary[:sharers]
json.data_sets summary[:data_sets]
json.data_sinks summary[:data_sinks]
json.tags @data_set.tags_list