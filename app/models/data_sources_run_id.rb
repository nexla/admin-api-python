class DataSourcesRunId < ActiveRecord::Base
    
    belongs_to :data_source
    scope :ordered, -> { order("created_at DESC") }
end