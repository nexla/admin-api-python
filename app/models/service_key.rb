class ServiceKey < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AuditLog

  belongs_to :owner, class_name: 'User'
  belongs_to :user
  belongs_to :org
  belongs_to :data_source

  enum status: { init: 'INIT', paused: 'PAUSED', active: 'ACTIVE' }

  before_save :handle_before_save

  def self.build_from_input(user, org, input)
    key = ServiceKey.new
    key.owner = user
    key.user = user
    key.org = org
    key.data_source_id = input[:data_source_id] if input.key?(:data_source_id)
    key.name = input[:name]
    key.description = input[:description]
    key.status = :active
    key.save!
    key
  end

  def update_mutable! (api_user_info, input)
    self.name = input[:name] if input[:name].present?
    self.description = input[:description] if input.key?(:description)
    self.save!
  end

  def rotate!
    self.last_rotated_key = self.api_key
    self.last_rotated_at = Time.now
    self.api_key = nil
    self.save!
  end

  def pause!
    self.status = :paused
    self.save!
  end

  def activate!
    self.status = :active
    self.save!
  end

  def handle_before_save
    if (self.api_key.blank?)
      self.api_key = SecureRandom.uuid.gsub("-", "")
      self.external_id = SecureRandom.alphanumeric(32)
    end
  end

end
