module Api::V1::DataSinks
  class ProbeController < Api::V1::ProbeController
    setup_probe_service_for(DataSink)
  end
end
