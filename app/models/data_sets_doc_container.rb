class DataSetsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_set
  belongs_to :doc_container
end