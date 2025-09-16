class ClustersOrg < ApplicationRecord
  belongs_to :cluster
  belongs_to :org_id
end