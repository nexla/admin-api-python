module Api::V1::DataCredential
  class ProbeController < Api::V1::ProbeController
    setup_probe_service_for(DataCredentials)
  end
end
