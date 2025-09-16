path = @api_root + "auth_parameters/#{@brief ? 'brief' : 'show'}"
json.partial! path, auth_param: @auth_param
