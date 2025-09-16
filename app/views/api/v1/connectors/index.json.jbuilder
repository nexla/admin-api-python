json.array! @connectors do |connector|
  json.(connector,
    :id,
    :type,
    :connection_type,
    :name,
    :description,
    :nexset_api_compatible
  )
end