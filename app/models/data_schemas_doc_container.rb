class DataSchemasDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_schema
  belongs_to :doc_container
end