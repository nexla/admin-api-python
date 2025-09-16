path = @api_root + "vendors/#{@brief ? 'brief' : 'show'}"
json.partial! path, vendor: @vendor
