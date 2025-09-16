class EndpointMapping < ApplicationRecord
  include AuditLog

  belongs_to :data_source, required: true
  belongs_to :data_set, required: true
end
