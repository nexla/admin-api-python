class Vendor < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor
  include AuditLog
  include Docs

  json_accessor :config
  has_many :auth_templates, dependent: :destroy
  has_many :vendor_endpoints, dependent: :destroy
  belongs_to :connector

  validates :name, uniqueness: true

  def self.build_from_input (input)
    auth_templates = input[:auth_templates]
    input.delete(:auth_templates)
    connection_type = input[:connection_type]
    connector = Connector.find_by_type(connection_type)
    raise Api::V1::ApiError.new(:bad_request, "Invalid connection type") if connector.nil?

    non_empty_templates = []
    if !auth_templates.blank?
      non_empty_templates = auth_templates.filter{|a| !a.blank?}
      Vendor.validate_auth_templates(non_empty_templates, connector)
    end

    vendor = Vendor.new(input)
    vendor.connector = connector
    non_empty_templates.each do |template|
      template[:connector] = vendor.connector
      auth_params = template[:params]
      template.delete(:params)
      auth_template = vendor.auth_templates.build(template)

      auth_params.each do |param|
        param[:vendor] = vendor
        auth_template.auth_parameters.build(param)
      end if !auth_params.blank?
    end

    vendor.save!
    return vendor
  end

  def update_mutable! (input)
    return if input.nil?

    if !input[:connection_type].blank?
      raise Api::V1::ApiError.new(:bad_request, "Cannot change connection type once it is set")
    end

    auth_templates = input[:auth_templates]
    input.delete(:auth_templates)
    non_empty_templates = []
    if !auth_templates.blank?
      non_empty_templates = auth_templates.filter{|a| !a.blank?}
      Vendor.validate_auth_templates(non_empty_templates, self.connector)
    end

    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if !input[:description].blank?
    self.config = input[:config] if !input[:config].blank?
    self.small_logo = input[:small_logo] if !input[:small_logo].blank?
    self.logo = input[:logo] if !input[:logo].blank?
    
    self.transaction do
      non_empty_templates.each do |template|
        existing_template = self.auth_templates.find {|t| template.key?(:id) && t.id == template[:id]}
        template.delete(:id)
        auth_params = template[:params]
        template.delete(:params)
        template[:connector] = self.connector
        if existing_template.nil?
          existing_template = self.auth_templates.build(template)
        else
          existing_template.update_mutable!(template)
        end

        auth_params.each do |param|
          existing_param = existing_template.auth_parameters.find {|t| param.key?(:id) && t.id == param[:id]}
          param.delete(:id)
          if existing_param.nil?
            param[:vendor_id] = self.id
            param[:auth_template_id] = existing_template.id
            existing_template.auth_parameters.build(param)
          else
            existing_param.update_mutable!(param)
          end
        end if !auth_params.blank?
      end if !auth_templates.blank?
      self.save!
    end   
  end

  def self.validate_auth_templates(auth_templates, vendor_connector)
    invalid=[]
    auth_templates.each do |template|
      if !template[:credentials_type].blank?
        connector = Connector.find_by_type(template[:credentials_type])
        if connector != vendor_connector
          invalid << template
        end
      end
    end

    raise Api::V1::ApiError.new(:bad_request, "Credentials type for these auth template did not match vendor's: #{invalid}") if !invalid.empty?
  end
end
