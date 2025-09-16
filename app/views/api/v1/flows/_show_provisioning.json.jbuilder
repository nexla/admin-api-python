
json.code_containers @resources[:code_containers] do |cc|
  json.(cc, :id, :owner_id, :org_id, :name, :public, :managed, :reusable, :resource_type,
    :output_type, :code_type, :code_encoding, :description, :tags, :copied_from_id,
    :created_at, :updated_at
  )
  if cc.data_credentials
    json.data_credentials do
      json.partial! @api_root + "provisioning/data_credentials", { data_credentials: cc.data_credentials }
    end
  else
    json.data_credentials nil
  end
end

json.data_sets @resources[:data_sets] do |dset|
  json.(dset, :id, :status, :data_source_id, :parent_data_set_id, :transform)
end

json.data_credentials @resources[:data_credentials] do |dc|
  json.partial! @api_root + "provisioning/data_credentials", { data_credentials: dc }
end

json.dependent_data_sources do
  json.array! @resources[:dependent_data_sources] do |dsrc|
    json.(dsrc, :id, :owner_id, :org_id)
    json.data_credentials do
      if dsrc.data_credentials
        json.(dsrc.data_credentials, :id, :credentials_enc, :credentials_enc_iv)
      else
        nil
      end
    end
    json.data_sink do
      if dsrc.data_sink
        json.(dsrc.data_sink, :id, :owner_id, :org_id)
      else
        nil
      end
    end
    json.(dsrc, :name, :description, :status, :source_type, :connector_type, :connection_type, :managed, :auto_generated)
    if (!dsrc.vendor_endpoint.nil? && !dsrc.vendor_endpoint.vendor.nil?)
      json.vendor do
        json.partial! @api_root + "vendors/brief", vendor: dsrc.vendor_endpoint.vendor
      end
    else
      json.vendor nil
    end
    json.(dsrc, :copied_from_id, :updated_at, :created_at)
  end
end

json.origin_data_sinks do
  json.array! @resources[:origin_data_sinks]
end

json.shared_data_sets do
  json.array! @resources[:shared_data_sets] do |dset|
    json.(dset, :id, :status, :data_source_id, :parent_data_set_id, :transform)
  end
end

json.provisioning_flow true
json.data_source do
  json.partial! @api_root + "provisioning/data_source", { data_source: @resources[:data_source] }
end

json.data_sink do
  json.partial! @api_root + "provisioning/data_sink", { data_sink: @resources[:data_sink] }
end
