class Runtime < ApplicationRecord
  include Api::V1::Schema
  include AuditLog
  include JsonAccessor
  include ReferencedResourcesConcern

  DEFAULT_STATUS = 'deactivated'

  enum status: { active: 'active', deactivated: 'deactivated' }

  json_accessor :config

  scope :active, -> { where(active: true) }

  belongs_to :org
  belongs_to :owner, class_name: "User"
  belongs_to :data_credentials

  validates :name, presence: true
  validates :dockerpath, presence: true

  mark_as_referenced_resource

  def self.build_from_input(user, org, input)
    raise Api::V1::ApiError.new(:bad_request, "Name is required") if input[:name].blank?

    runtime = Runtime.new
    runtime.owner_id = user.id
    runtime.org_id = org.id
    runtime.update_mutable(user, input)
    runtime
  end

  def update_mutable(user, input)
    input = input.symbolize_keys
    self.name = input[:name]
    self.description = input[:description] if input.key?(:description)
    self.active = input[:active] if input.key?(:active)
    self.dockerpath = input[:dockerpath] if input[:dockerpath].present?

    if input.key?(:data_credentials_id)
      data_credentials = DataCredentials.find_by(id: input[:data_credentials_id])
      unless data_credentials
        raise Api::V1::ApiError.new(:not_found, "Data credentials not found")
      end

      unless data_credentials.has_collaborator_access?(user)
        raise Api::V1::ApiError.new(:forbidden, "Data credentials not accessible")
      end
      self.data_credentials_id = data_credentials.id
    end

    self.managed = input[:managed] if input.key?(:managed)
    self.config = input[:config] if input.key?(:config)
    self.save!
  end

  def deactivate!
    self.active = false
    self.save!
  end

  def activate!
    self.active = true
    self.save!
  end

end
