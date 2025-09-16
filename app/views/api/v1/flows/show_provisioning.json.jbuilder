json.array! [{}] do
  # Preserving the odd response format for backwards
  # compatibility: an array with one object in it
  # containing the details for the provisioned flow.
  json.partial! @api_root + "flows/show_provisioning"
end
