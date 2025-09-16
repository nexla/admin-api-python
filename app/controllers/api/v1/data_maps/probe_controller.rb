module Api::V1::DataMaps
  class ProbeController < Api::V1::ProbeController
    setup_probe_service_for(DataMap)
  end
end
