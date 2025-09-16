path = @api_root + "data_credentials/#{@brief ? 'brief' : 'index_show'}"
json.array! @data_credentials, partial: path, as: :data_credentials
