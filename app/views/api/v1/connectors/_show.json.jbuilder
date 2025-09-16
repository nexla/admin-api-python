if connector
  json.connector do
    json.(connector,
      :id,
      :type,
      :connection_type,
      :name,
      :description,
      :nexset_api_compatible
    )
  end
else
  json.set! :connector, {}
end
