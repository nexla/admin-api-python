class DataSourcesDataSet < ApplicationRecord
  belongs_to :data_source
  belongs_to :data_set
end