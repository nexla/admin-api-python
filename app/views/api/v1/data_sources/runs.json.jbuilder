json.array!(@run_ids) do |run|
  json.id run.run_id
  json.created_at run.created_at
end
