# frozen_string_literal: true

class ResourceConfig::InputValidator
  def initialize(input)
    @input = input
  end

  attr_reader :input

  VALID_CONNECTOR_TYPES = VendorEndpoint::Connection_Types.values_at(:script, :s3).freeze

  def valid_script_config?
    if (input.key?(:vendor_endpoint_id))
      VALID_CONNECTOR_TYPES.include?(vendor_endpoint&.vendor&.connection_type)
    else
      false
    end
  end

  def valid_template_config?
    (input.key?(:vendor_endpoint_id) || input.key?(:vendor_endpoint)) && input.key?(:template_config)
  end

  def vendor_endpoint
    VendorEndpoint.find_by_id(input[:vendor_endpoint_id])
  end
end
