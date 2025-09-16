module Api::V1::DataSources
  class ProbeController < Api::V1::ProbeController
    setup_probe_service_for(DataSource)
  end
end
