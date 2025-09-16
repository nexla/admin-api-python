class DataSourcesDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_sources
  belongs_to :doc_container
end