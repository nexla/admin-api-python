path = @api_root + "auth_templates/#{@brief ? 'brief' : 'show'}"
json.partial! path, auth_template: @auth_template