class ResourceParameter < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  belongs_to :vendor_endpoint

  json_accessor :config, :allowed_values

  def self.build_from_input (input)
    if (input.key?(:vendor_endpoint_name))
      vendor_endpoint = VendorEndpoint.find_by_name(input[:vendor_endpoint_name])
      input[:vendor_endpoint_id] = vendor_endpoint.id if !vendor_endpoint.nil?
      input.delete(:vendor_endpoint_name)
    end

    if input.key?(:vendor_endpoint_id)
      vendor_endpoint = VendorEndpoint.find_by_id(input[:vendor_endpoint_id])
      raise Api::V1::ApiError.new(:not_found, "Invalid vendor endpoint") if vendor_endpoint.nil?
    end

    if !(input.key?(:vendor_endpoint_id)) && (!input.key?(:global) || !input[:global])
      raise Api::V1::ApiError.new(:bad_request, "Vendor endpoint is required or parameter should be global")
    elsif (input.key?(:vendor_endpoint_id) && input.key?(:global) && !!input[:global])
      raise Api::V1::ApiError.new(:bad_request, "Parameter can not be global and have a vendor endpoint")
    end

    resource_param = ResourceParameter.create(input)
    return resource_param

  end

  def update_mutable! (input)
    return if input.nil?

    if (input.key?(:vendor_endpoint_name))
      vendor_endpoint = VendorEndpoint.find_by_name(input[:vendor_endpoint_name])
      input[:vendor_endpoint_id] = vendor_endpoint.id if !vendor_endpoint.nil?
      input.delete(:vendor_endpoint_name)
    end

    if input.key?(:vendor_endpoint_id)
      vendor_endpoint = VendorEndpoint.find_by_id(input[:vendor_endpoint_id])
      raise Api::V1::ApiError.new(:not_found, "Invalid vendor endpoint") if vendor_endpoint.nil?
    end

    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if !input[:description].blank?
    self.resource_type = input[:resource_type] if !input[:resource_type].blank?
    self.vendor_endpoint_id = input[:vendor_endpoint_id] if !input[:vendor_endpoint_id].blank?
    self.data_type = input[:data_type] if !input[:data_type].blank?
    self.order = input[:order] if !input[:order].blank?
    self.allowed_values = input[:allowed_values] if !input[:allowed_values].blank?
    self.config = input[:config] if !input[:config].blank?

    if input.key?(:global)
      self.global = input[:global]
      self.vendor_endpoint_id = nil if self.global
    end

    if self.vendor_endpoint_id.nil? && !self.global
      raise Api::V1::ApiError.new(:bad_request, "Vendor endpoint is required or parameter should be global")
    elsif !self.vendor_endpoint_id.nil? && !self.global.nil? && self.global
      raise Api::V1::ApiError.new(:bad_request, "Parameter can not have vendor endpoint and global both")
    end
    self.save!
  end

  def self.global_resource_parameters
    ResourceParameter.where(:global => true)
  end
end
