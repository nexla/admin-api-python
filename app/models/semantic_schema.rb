class SemanticSchema < ApplicationRecord
  self.primary_key = :id

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :data_set

  # Note, semantic_schemas table is append ONLY. We never modify
  # existing records. 
  #
  # There is no separate _versions table: all new entries are
  # recorded as part of 'create' or 'update' events in data_set_versions.
  #
  # A data_set association is required. semantic_schemas cannot be used for 
  # storing arbitrary schemas without a corresponding data_set_id. 
  
  def self.create_for_data_set (data_set, schema)
    raise Api::V1::ApiError.new(:internal_server_error,
      "Semantic schema must be a hash object") if !schema.is_a?(Hash)

    SemanticSchema.create({
      owner_id: data_set.owner.id,
      org_id: data_set.org&.id,
      data_set_id: data_set.id,
      schema: schema
    })
  end

  def schema
    return {} if read_attribute(:schema).blank?
    super
  end
end