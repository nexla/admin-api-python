class OrgsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :org
  belongs_to :doc_container
end