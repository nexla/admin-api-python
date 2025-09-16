class DataSetsParentDataSet < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_set
  belongs_to :parent_data_set, class_name: "DataSet"
end