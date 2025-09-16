class VendorsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :vendor
  belongs_to :doc_container
end