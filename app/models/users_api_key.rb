class UsersApiKey < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include ApiKeyCommon
  include SearchableConcern

  belongs_to :user, required: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org
  has_one :data_credential, class_name: "DataCredentials"

  before_save :handle_before_save

  Scopes = {
    :all => "all",
    :nexla_monitor => DataSource.connector_types[:nexla_monitor]
  }

  Nexla_Monitor_Scopes = [ Scopes[:all], Scopes[:nexla_monitor] ]

  def url
    nil
  end
end
