json.array! @resource.sso_options do |sso|
  json.extract! sso, *sso.keys
end