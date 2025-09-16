class CodeContainersDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :code_container
  belongs_to :doc_container
end