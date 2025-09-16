class VendorEndpoint < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  belongs_to :vendor
  has_many :resource_parameters
  has_many :data_sources
  has_many :data_sinks

  has_many :source_flow_nodes, through: :data_sources, source: :flow_node, class_name: 'FlowNode'
  has_many :sink_flow_nodes, through: :data_sinks, source: :flow_node, class_name: 'FlowNode'

  validates :name, uniqueness: true

  json_accessor :source_template, :sink_template, :config

  Connection_Types = {
      :rest        => 'rest',
      :script      => 'script', # Deprecated, we use s3 now
      :s3 => 's3', # Alias of script, as of 5/22
      :api_streams => 'api_streams', # Alias for rest
      :api_multi => 'api_multi' # Alias for rest
  }

  def self.build_from_input (input)
    vendor = nil
    if (input.key?(:source_template) && input.key?(:sink_template))
      raise Api::V1::ApiError.new(:bad_request, "Vendor endpoint can not have source and sink template both")
    end

    if (input.key?(:vendor_name))
      vendor = Vendor.find_by_name(input[:vendor_name])
      input.delete(:vendor_name)
    elsif (input.key?(:vendor_id))
      vendor = Vendor.find_by_id(input[:vendor_id])
    else
      raise Api::V1::ApiError.new(:bad_request, "Vendor name or id is required")
    end

    raise Api::V1::ApiError.new(:bad_request, "Invalid vendor") if vendor.nil?
    if Connection_Types.find { |sym, str| str == vendor.connector.type }.nil?
      raise Api::V1::ApiError.new(:bad_request, "We only support rest and script connectors for source and sink templatization")
    end

    input[:vendor_id] = vendor.id
    vendor_endpoint = VendorEndpoint.create(input)
    vendor_endpoint.save!
    return vendor_endpoint
  end

  def connection_type
    self.vendor&.connection_type
  end

  def update_mutable! (input)
    return if input.nil?

    if (input.key?(:vendor_name))
      vendor = Vendor.find_by_name(input[:vendor_name])
      input[:vendor_id] = vendor.id if !vendor.nil?
      input.delete(:vendor_name)
    end

    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if !input[:description].blank?

    if !input[:vendor_id].blank?
      vendor = Vendor.find_by_id(input[:vendor_id])
      raise Api::V1::ApiError.new(:not_found, "Invalid vendor") if vendor.nil?
      self.vendor_id = input[:vendor_id]
    end

    if ((!self.source_template.blank? && input.key?(:sink_template)) ||
        (!self.sink_template.blank? && input.key?(:source_template)) ||
        (input.key?(:source_template) && input.key?(:sink_template)))
      raise Api::V1::ApiError.new(:bad_request, "Vendor endpoint can not have source and sink templates both")

    end

    self.source_template = input[:source_template] if !input[:source_template].blank?
    self.sink_template = input[:sink_template] if !input[:sink_template].blank?
    self.config = input[:config] if !input[:config].blank?
    self.save!
  end

  def elt_endpoint?
    ['api_streams', 'api_multi'].include? vendor&.connector&.type
  end

  def self.get_resource_config(data_credentials, input, resource_type, config, resource_id = nil, email_token = nil, template_type = "api", enable_validation = true)
    vendor_endpoint = input.key?(:vendor_endpoint) ? VendorEndpoint.find_by_name(input[:vendor_endpoint]) :
                         VendorEndpoint.find(input[:vendor_endpoint_id])

    if vendor_endpoint.elt_endpoint?
      ResourceConfig.new(data_credentials: data_credentials, input: input, resource_type: resource_type, config: config, resource_id: resource_id, email_token: email_token, template_type: template_type, enable_validation: enable_validation).get
    else
      old_get_resource_config(data_credentials, input, resource_type, config, resource_id, email_token, template_type, enable_validation)
    end
  end

  def self.old_get_resource_config(data_credentials, input, resource_type, config, resource_id = nil, email_token = nil, template_type = "api", enable_validation = true)
    vendor_endpoint = input.key?(:vendor_endpoint) ? VendorEndpoint.find_by_name(input[:vendor_endpoint]) :
                         VendorEndpoint.find(input[:vendor_endpoint_id])

    raise Api::V1::ApiError.new(:bad_request, "Invalid vendor endpoint") if vendor_endpoint.nil?
    resource_template = (resource_type == 'SOURCE') ? vendor_endpoint.source_template : vendor_endpoint.sink_template

    template_config = config
    template_config.merge!(data_credentials.template_config(true)) if !data_credentials.nil?

    parameter_list = []

    auth_params = AuthParameter.where("vendor_id = :vendor_id or global = :global", {vendor_id: vendor_endpoint.vendor.id, global: true}).map(&:name)
    resource_params = ResourceParameter.where("(vendor_endpoint_id = :vendor_endpoint_id or global = :global) and (resource_type = :resource_type)",
                                              {vendor_endpoint_id: vendor_endpoint.id, global: true, resource_type: resource_type}).map(&:name)

    parameter_list.push(*auth_params)
    parameter_list.push(*resource_params)

    data_credentials&.add_data_cred_params(parameter_list, data_credentials.credentials)

    parameter_list = (parameter_list + ["source_id", "sink_id", "email_token"]) if template_type == "script"

    resource_config = {}
    resource_template.each do |key, value|
      # TODO: This macro replacement is not scaleable. Need to update it with a better replacement logic.
      if ((key == "rest.iterations" && value.is_a?(Array)) || (key == "credentials" && value.is_a?(Array)))
        iteration_array = []
        parent_index = 0
        value.each do |iteration|
          if iteration.is_a?(Hash)
            iteration_config = {}
            iteration.each do |itr_key, itr_value|
              iteration_config = replace_config_from_template(itr_key, itr_value, parameter_list, template_config, iteration_config, parent_index)
            end
            iteration_array.push(iteration_config)
          end
          parent_index = parent_index.to_i + 1
        end
        resource_config[key] = iteration_array
      elsif ((key == "parameters" || key == "mapping") && value.is_a?(Hash))
        parameters_map = {}
        value.each do |itr_key, itr_value|
          parameters_map = replace_config_from_template(itr_key, itr_value, parameter_list, template_config, parameters_map, 0,
                                                        resource_id, email_token)
        end
        resource_config[key] = parameters_map
      else
        resource_config = replace_config_from_template(key, value, parameter_list, template_config, resource_config, 0, resource_id, email_token)
      end
    end

    validate_config(resource_config, parameter_list) if enable_validation
    return resource_config, vendor_endpoint
  end

  def self.validate_config(config, parameter_list)
    config.each do |key, value|
      parameter_list.each do |param|
        if value.to_s.include?("${#{param}}")
          raise Api::V1::ApiError.new(:bad_request, "Missing config #{param} for template")
        end
      end
    end
  end

  class << self
    def valid_script_config_parameters? (input)
      ResourceConfig::InputValidator.new(input).valid_script_config?
    end

    def valid_template_config_parameters? (input)
      ResourceConfig::InputValidator.new(input).valid_template_config?
    end

    def replace_config_from_template(key, input_value, parameter_list, template_config, config, parent_index = 0, resource_id = nil, email_token = nil)
      config[key] = self.map_parameter(input_value, parameter_list, template_config, parent_index, resource_id, email_token)
      return config
    end
  end

  def self.map_parameter(input_value, parameter_list, template_config, parent_index, resource_id, email_token)
    if input_value.is_a?(Hash)
      mapped_values = {}
      input_value.each do |itr_key, itr_value|
        mapped_value = self.map_parameter(itr_value, parameter_list, template_config, parent_index, resource_id, email_token)
        mapped_values[itr_key] = mapped_value
      end
      return mapped_values
    elsif input_value.is_a?(Array)
      mapped_values = []
      input_value.each do |itr_value|
        mapped_values << self.map_parameter(itr_value, parameter_list, template_config, parent_index, resource_id, email_token)
      end
      return mapped_values
    end
    input_str = input_value.to_s

    if input_str.include?("${")
      should_convert_to_int = input_str.starts_with?("${") && input_str.ends_with?("}.as_integer") ? true : false
      template_val = input_str
      template_val = input_str.chomp!(".as_integer") if should_convert_to_int

      #TODO: Too much parsing of same parameters going on here. Need to optimize
      parameter_list.each do |parameter|
        template_value_mapped = nil
        if parameter.to_s.include?("rest.iterations")
          iteration_param_name = parameter.gsub("rest.iterations.","")
          template_iteration = template_config["rest.iterations"]

          if !template_iteration.nil? && template_iteration.is_a?(Array)
            iteration = template_iteration[parent_index]
            if !iteration.nil? && iteration.is_a?(Hash)
              iteration.each do |itr_key, itr_value|
                if itr_key == iteration_param_name
                  template_value_mapped = itr_value
                end
              end
            end
          end
        elsif (!resource_id.nil? && template_config[parameter].blank? &&
          (parameter.to_s == "source_id" || parameter.to_s == "sink_id" || parameter.to_s == "email_token"))
          if parameter.to_s == "source_id" || parameter.to_s == "sink_id"
            template_value_mapped = resource_id.to_s
          elsif parameter.to_s == "email_token"
            template_value_mapped = email_token
          end
        elsif !template_config[parameter].nil?
          template_value_mapped = template_config[parameter]
        end

        if !template_value_mapped.nil?
          template_key = "${#{parameter}}"
          template_val = template_val.gsub(template_key,template_value_mapped.to_s)
        end
      end

      #TODO: probably need a better method of ensuring the final resource config can have non-string datatypes.
      #if value is of format "${...}.as_integer" then convert to integer.
      return should_convert_to_int ? template_val.to_i : template_val
    else
      return input_str
    end
  end

  def resource_type
    sink? ? "SINK" : "SOURCE"
  end

  def sink?
    self.sink_template.present?
  end

  def source?
    !sink?
  end

  def flow_nodes
    if source?
      source_flow_nodes
    else
      sink_flow_nodes
    end
  end
end
