path = @api_root + "resource_parameters/#{@brief ? "brief" : "show"}"
json.partial! path, resource_param: @resource_param
