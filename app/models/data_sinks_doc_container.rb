class DataSinksDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_sink
  belongs_to :doc_container
end