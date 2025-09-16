class CodeContainersDataMap < ApplicationRecord
  self.primary_key = :id
  
  belongs_to :code_container
  belongs_to :data_map
end