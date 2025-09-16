class AuthParameter < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  belongs_to :vendor
  belongs_to :auth_template

  json_accessor :config, :allowed_values

  def self.build_from_input (input)

    if (input.key?(:auth_template_name))
      auth_template = AuthTemplate.find_by_name!(input[:auth_template_name])
      input[:auth_template_id] = auth_template.id if !auth_template.nil?
      input.delete(:auth_template_name)
    elsif input.key?(:auth_template_id)
      auth_template = AuthTemplate.find_by_id(input[:auth_template_id])
      raise Api::V1::ApiError.new(:not_found, "Invalid auth template") if auth_template.nil?
    end

    if auth_template.nil?
      if (input.key?(:vendor_name))
        vendor = Vendor.find_by_name!(input[:vendor_name])
        input[:vendor_id] = vendor.id if !vendor.nil?
        input.delete(:vendor_name)
      end

      if input.key?(:vendor_id)
        vendor = Vendor.find_by_id(input[:vendor_id])
        raise Api::V1::ApiError.new(:not_found, "Invalid vendor") if vendor.nil?
      end

      if !vendor.nil?
        if vendor.auth_templates.count == 1
          auth_template = vendor.auth_templates.first
          input[:auth_template_id] = auth_template.id
        else
          raise Api::V1::ApiError.new(:bad_request, "Vendor has multiple auth templates. Please specify an auth template")
        end
      end
    end

    if auth_template.nil? && (!(input.key?(:global)) || !input[:global])
      raise Api::V1::ApiError.new(:bad_request, "Either an auth template is required, a vendor with only one auth template is required, or parameter should be global")
    end
    if (!auth_template.nil? && input.key?(:global) && input[:global])
      raise Api::V1::ApiError.new(:bad_request, "Parameter can not have an auth template and be global")
    end

    input[:vendor_id] = auth_template.vendor_id if !auth_template.nil?

    auth_param = AuthParameter.create(input)
    return auth_param
  end

  def update_mutable! (input)
    return if input.nil?

    if (input.key?(:auth_template_name))
      auth_template = AuthTemplate.find_by_name!(input[:auth_template_name])
      input[:auth_template_id] = auth_template.id if !auth_template.nil?
      input[:vendor_id] = auth_template.vendor_id if !auth_template.nil?
      input.delete(:auth_template_name)
    elsif input.key?(:auth_template_id)
      auth_template = AuthTemplate.find_by_id(input[:auth_template_id])
      input[:vendor_id] = auth_template.vendor_id if !auth_template.nil?
      raise Api::V1::ApiError.new(:not_found, "Invalid auth template") if auth_template.nil?
    end

    if auth_template.nil?
      if (input.key?(:vendor_name))
        vendor = Vendor.find_by_name!(input[:vendor_name])
        input[:vendor_id] = vendor.id if !vendor.nil?
        input.delete(:vendor_name)
      end

      if input.key?(:vendor_id)
        vendor = Vendor.find_by_id(input[:vendor_id])
        raise Api::V1::ApiError.new(:not_found, "Invalid vendor") if vendor.nil?
      end

      if !vendor.nil?
        if vendor.auth_templates.count == 1
          auth_template = vendor.auth_templates.first
          input[:auth_template_id] = auth_template.id
        else
          raise Api::V1::ApiError.new(:bad_request, "Vendor has multiple auth templates. Please specify an auth template")
        end
      end
    end

    input[:vendor_id] = auth_template.vendor_id if !auth_template.nil?

    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if !input[:description].blank?
    self.vendor_id = input[:vendor_id] if !input[:vendor_id].blank?
    self.auth_template_id = input[:auth_template_id] if !input[:auth_template_id].blank?

    # This only exists in case we're updating an old parameter which should have a template but doesn't
    if self.auth_template_id.nil? && !self.vendor_id.nil?
      vendor = Vendor.find self.vendor_id
      if vendor.auth_templates.count > 0
        self.auth_template_id = vendor.auth_templates.first.id
      end
    end

    self.data_type = input[:data_type] if !input[:data_type].blank?
    self.order = input[:order] if !input[:order].blank?
    self.allowed_values = input[:allowed_values] if !input[:allowed_values].blank?

    if input.key?(:global)
      self.global = input[:global]
      self.vendor_id = nil if self.global
      self.auth_template_id = nil if self.global
    end
    self.config = input[:config] if !input[:config].blank?
    self.secured = input[:secured] if !input[:secured].blank?

    if self.auth_template_id.nil? && !self.global
      raise Api::V1::ApiError.new(:bad_request, "Auth template and vendor id is required or parameter should be global")
    elsif !self.auth_template_id.nil? && !self.global.nil? && self.global
      raise Api::V1::ApiError.new(:bad_request, "Auth parameter cannot be global and have a vendor/template")
    end
    self.save!
  end

  def self.global_auth_parameters
    AuthParameter.where(global: true)
  end
end
