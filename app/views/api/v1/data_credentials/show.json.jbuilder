path = @api_root + "data_credentials/#{@brief ? 'brief' : 'show'}"
json.partial! path, data_credentials: @data_credentials
