class DataSinksApiKey < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include ApiKeyCommon
  include SearchableConcern

  belongs_to :data_sink, required: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org

  before_save :handle_before_save

  def url
    nil
  end
end
