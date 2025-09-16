class ResourceConfig::MapParameter
  def initialize(input_value:, parameter_list:, template_config:, parent_index: 0, resource_id: nil, email_token: nil)
    @input_value = input_value
    @parameter_list = parameter_list
    @template_config = template_config
    @parent_index = parent_index
    @resource_id = resource_id
    @email_token = email_token
  end

  attr_reader :input_value, :parameter_list, :template_config, :parent_index, :resource_id, :email_token

  def parse
    case input_value
    when Hash then parse_hash
    when Array then parse_array
    else parse_single_attribute
    end
  end

  private

  def parse_hash
    input_value.transform_values do |itr_value|
      map_parameter(itr_value)
    end
  end

  def parse_array
    input_value.map do |itr_value|
      map_parameter(itr_value)
    end
  end

  def map_parameter(value)
    self.class.new(input_value: value, parameter_list: parameter_list, template_config: template_config, parent_index: parent_index, resource_id: resource_id, email_token: email_token).parse
  end

  def parse_single_attribute
    return input_value if [Integer, Float, NilClass, TrueClass, FalseClass].any? { |klass| input_value.is_a?(klass) }

    input_str = input_value.to_s

    if input_str.include?("${")
      should_convert_to_int = input_str.starts_with?("${") && input_str.ends_with?("}.as_integer") ? true : false
      input_str.gsub(/\$\{([\w\s\.-]+)\}(\.as_integer)?/) do |matched_string|
        parse_parameter($1) || matched_string
      end.yield_self { |value| should_convert_to_int ? value.to_i : value}
    else
      return input_str
    end
  end

  def parse_parameter(parameter)
    case
    when parameter.include?('rest.iterations')
      iteration_param_name = parameter.sub('rest.iterations.', '')
      template_config.dig('rest.iterations', parent_index, iteration_param_name) rescue TypeError
    when !parameter_list.include?(parameter)
      nil # Only replace parameters when we include them in the list
    when template_config[parameter].present? || template_config.dig('params', parameter).present?
      template_config[parameter] || template_config.dig('params', parameter)
    when parameter == 'email_token'
      email_token
    when resource_id.present? && ['source_id', 'sink_id'].include?(parameter)
      resource_id.to_s
    end
  end
end
