class CustomDataFlowsCodeContainer < ApplicationRecord
  self.primary_key = :id

  belongs_to :custom_data_flow
  belongs_to :code_container
end