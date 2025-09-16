module Api::V1
  class NotificationChannelSettingsController < Api::V1::ApiController

    def index
      @notification_channel_setting = current_user.notification_channel_settings(request_access_role, current_org).page(@page).per_page(@per_page)
      set_link_header(@notification_channel_setting)
    end

    def show
      return if render_schema NotificationChannelSetting
      @notification_channel_setting = NotificationChannelSetting.find(params[:id])
      authorize! :read, @notification_channel_setting
    end

    def create
      input = (validate_body_json NotificationChannelSetting).symbolize_keys
      @notification_channel_setting = NotificationChannelSetting.build_from_input(input, current_user, current_org)
      render "show"
    end

    def update
      input = (validate_body_json NotificationChannelSetting).symbolize_keys

      @notification_channel_setting = NotificationChannelSetting.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @notification_channel_setting.nil?
      authorize! :manage, @notification_channel_setting

      @notification_channel_setting.update_mutable!(request, current_user, input)
      render "show"
    end

    def destroy
      head :method_not_allowed
    end

  end
end
