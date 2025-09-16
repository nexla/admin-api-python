module ControlEventConcern
  extend ActiveSupport::Concern

  def show_control_event
    head :forbidden if !current_user.super_user?
    model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
    resource = model.find(params[:resource_id])
    if !["create", "activate", "pause", "update", "run_now", "delete"].include?(params[:event])
      raise Api::V1::ApiError.new(:bad_request, "Invalid control event")
    end
    r = ControlService.new(resource).show_publish(params[:event].to_sym)
    render status: r[:status], json: (r[:status] == :ok) ? r[:output] : r[:message]
  end

end