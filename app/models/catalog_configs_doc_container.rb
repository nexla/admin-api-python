class CatalogConfigsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :catalog_config
  belongs_to :doc_container
end