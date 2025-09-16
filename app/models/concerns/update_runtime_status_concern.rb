module UpdateRuntimeStatusConcern
  extend ActiveSupport::Concern

  def update_runtime_status(runtime_status)
    if runtime_status.blank? || !runtime_status.to_s.upcase.in?(RUNTIME_STATUSES.values)
      raise Api::V1::ApiError.new(:bad_request, "Invalid runtime status: '#{runtime_status}'")
    end

    PaperTrail.request.disable_model(self.class)
    begin
      self.update(runtime_status: runtime_status.to_s.upcase)
    ensure
      PaperTrail.request.enable_model(self.class)
    end
  end

  RUNTIME_STATUSES = API_RUNTIME_STATUSES
end