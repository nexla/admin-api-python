class DataSetsApiKey < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include ApiKeyCommon
  include SearchableConcern

  belongs_to :data_set, required: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org

  before_save :handle_before_save

  def url
    EnvironmentUrl.instance.nexset_api_url(self.data_set, self.api_key)
  end
end
