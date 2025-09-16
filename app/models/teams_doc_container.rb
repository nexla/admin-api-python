class TeamsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :team
  belongs_to :doc_container
end