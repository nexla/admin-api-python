class AuthTemplate < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AuditLog
  include JsonAccessor

  belongs_to :vendor
  belongs_to :connector
  has_many :auth_parameters, dependent: :destroy

  json_accessor :config

  validates :name, uniqueness: true

  def self.build_from_input (input)
    if (input.key?(:vendor_name))
      vendor = Vendor.find_by_name(input[:vendor_name])
      input[:vendor_id] = vendor.id if !vendor.nil?
      input.delete(:vendor_name)
    elsif input.key?(:vendor_id)
      vendor = Vendor.find_by_id(input[:vendor_id])
      raise Api::V1::ApiError.new(:not_found, "Invalid vendor") if vendor.nil?
    else
      raise Api::V1::ApiError.new(:bad_request, "vendor or vendor_id input is required")
    end
    input[:connector_id] = vendor.connector_id
    auth_template = AuthTemplate.create(input)
    auth_template.save!
    return auth_template
  end

  def update_mutable! (input)
    self.config = input[:config] if !input[:config].blank?
    self.name = input[:name] if !input[:name].blank?
    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.description = input[:description] if input.key?(:description)
    self.save!
  end

  after_create do
    self.name ||= self.vendor.name + "_template_" + self.id.to_s
    self.display_name ||= self.vendor.name.camelize + " Template " + self.id.to_s
    self.save!
  end
end
