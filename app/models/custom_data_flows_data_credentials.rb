class CustomDataFlowsDataCredentials < ApplicationRecord
  self.primary_key = :id

  belongs_to :custom_data_flow
  belongs_to :data_credentials, class_name: "DataCredentials"
end