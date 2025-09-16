class DataMapsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_map
  belongs_to :doc_container
end