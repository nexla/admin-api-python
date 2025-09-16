path = @api_root + "vendors/#{@brief ? 'brief' : 'show'}"

json.array! @vendor do |vendor|
  json.partial! path, vendor: vendor
end