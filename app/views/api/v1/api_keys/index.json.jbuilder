
@api_keys.keys.each do |resource_type|

  json.set! resource_type, @api_keys[resource_type] do |api_key|
    resource_attribute = (resource_type.to_s.singularize + "_id").to_sym
    json.(api_key,
      :id,
      resource_attribute,
      :owner_id,
      :org_id,
      :name,
      :description,
      :status,
      :scope,
      :api_key,
      :url,
      :last_rotated_key,
      :last_rotated_at,
      :updated_at,
      :created_at
    )
  end

end