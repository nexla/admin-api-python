class ProjectsDocContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :project
  belongs_to :doc_container
end