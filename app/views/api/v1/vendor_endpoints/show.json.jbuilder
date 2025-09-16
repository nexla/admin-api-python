path = @api_root + "vendor_endpoints/#{@brief ? "brief" : "show"}"
json.partial! path, vendor_endpoint: @vendor_endpoint
