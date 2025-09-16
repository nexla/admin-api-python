class DataSourcesApiKey < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include ApiKeyCommon
  include SearchableConcern

  belongs_to :data_source, required: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org

  before_save :handle_before_save

  def url
    return self.data_source.webhook_url(self) if self.data_source&.webhook?
    return self.data_source.file_upload_url(self) if self.data_source&.file_upload?
    nil
  end
end
