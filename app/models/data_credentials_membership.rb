class DataCredentialsMembership < ApplicationRecord
  belongs_to :data_credentials_group
  belongs_to :data_credentials

  validates :data_credentials_id, uniqueness: { scope: :data_credentials_group_id }
end
