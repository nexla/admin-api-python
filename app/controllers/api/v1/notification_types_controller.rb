module Api::V1
  class NotificationTypesController < Api::V1::ApiController
    def index
      status_map = {
        "active" => 1,
        "ACTIVE" => 1,
        "pause" => 0,
        "PAUSE" => 0
      }
      status = status_map[params[:status]]
      if !status.nil?
        @notification_type = collection.where(:status => status).page(@page).per_page(@per_page)
      else
        @notification_type = collection.page(@page).per_page(@per_page)
      end
      set_link_header(@notification_type)
      render "index"
    end

    def list
      @notification_type = collection.where(:resource_type => params[:resource_type], :event_type => params[:event_type]).first
      render "list"
    end

    private

    def collection
      NotificationType.visible
    end
  end
end

