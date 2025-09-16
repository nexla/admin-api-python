class ResourceConfig
  extend Memoist

  def initialize(data_credentials:, input:, resource_type:, config:, resource_id: nil, email_token: nil, template_type: 'api', enable_validation: true)
    @data_credentials = data_credentials
    @input = input.with_indifferent_access
    @resource_type = resource_type
    @config = config
    @resource_id = resource_id
    @email_token = email_token
    @template_type = template_type
    @enable_validation = enable_validation
  end

  attr_reader :data_credentials, :input, :resource_type, :config, :resource_id, :email_token, :enable_validation, :template_type

  memoize def vendor_endpoint
    if input.key?(:vendor_endpoint) 
      VendorEndpoint.find_by(name: input[:vendor_endpoint])
    else
      VendorEndpoint.find_by(id: input[:vendor_endpoint_id])
    end
  end

  memoize def elt_flow?
    vendor_endpoint.elt_endpoint?
  end

  memoize def template_config
    config.merge!(data_credentials&.template_config(true) || {})
  end

  def get
    raise Api::V1::ApiError.new(:bad_request, "Invalid vendor endpoint") if vendor_endpoint.nil?

    data_credentials&.add_data_cred_params(parameter_list, data_credentials.credentials)

    validate_config(resource_config, parameter_list) if enable_validation
    return resource_config, vendor_endpoint
  end

  def resource_template
    ((resource_type == 'SOURCE') ? vendor_endpoint.source_template : vendor_endpoint.sink_template).tap do |merged|
      if elt_flow?
        merged.deep_merge!(feed_config) do |_, config_value, feed_value|
          if config_value.is_a?(Array) && feed_value.is_a?(Array)
            config_value + feed_value
          else
            config_value # Use existing value instead
          end
        end
      end
    end
  end

  def feed_config
    template_config.with_indifferent_access[:feeds_config] || {}
  end

  memoize def parameter_list
    (auth_params + resource_params).tap do |params|
      params.append("source_id", "sink_id", "email_token") if template_type == 'script'
    end
  end

  def auth_params
    AuthParameter.where("vendor_id = :vendor_id or global = :global", {vendor_id: vendor_endpoint.vendor.id, global: true}).pluck(:name)
  end

  def resource_params
    ResourceParameter.where("(vendor_endpoint_id = :vendor_endpoint_id or global = :global) and (resource_type = :resource_type)",
                                              {vendor_endpoint_id: vendor_endpoint.id, global: true, resource_type: resource_type}).pluck(:name)
  end

  memoize def resource_config
    resource_template.map do |key, value|
      [key, transform_value(key, value)]
    end.to_h
  end

  def transform_value(key, value)
    if ((key == "rest.iterations" && value.is_a?(Array)) || (key == "credentials" && value.is_a?(Array)))
      value.map.with_index do |iteration, parent_index|
        if iteration.is_a?(Hash)
          iteration.transform_values { |itr_value| map_parameter(input_value: itr_value, parent_index: parent_index) }
        end
      end.compact
    elsif ((key == "parameters" || key == "mapping") && value.is_a?(Hash))
      value.transform_values { |itr_value| map_parameter(input_value: itr_value) }
    else
      map_parameter(input_value: value)
    end
  end

  def map_parameter(input_value:, parent_index: 0)
    ResourceConfig::MapParameter.new(input_value: input_value, parameter_list: parameter_list, template_config: template_config, parent_index: parent_index, resource_id: resource_id, email_token: email_token).parse
  end

  def validate_config(config, parameter_list)
    config.each do |key, value|
      parameter_list.each do |param|
        if value.to_s.include?("${#{param}}")
          raise Api::V1::ApiError.new(:bad_request, "Missing config #{param} for template")
        end
      end
    end
  end
end
