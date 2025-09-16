module Api::V1
  class NotificationSettingsController < Api::V1::ApiController

    include ControlEventConcern

    def index
      sort_order = (params[:sort_order] || 'ASC')
      sort_by = ((params[:sort_by] || 'priority')) + " #{sort_order}"
      condition = {}
      if (params.key?(:notification_resource_type))
        condition[:notification_resource_type] = params[:notification_resource_type]
      end
      if (params.key?(:resource_id))
        condition[:resource_id] = params[:resource_id]
      end

      notification_select = NotificationSetting.where(:owner_id => current_user.id, :org_id => current_org.id)

      if !(condition.empty?)
        notification_select = notification_select.where(condition)
      end
      @notification_setting = notification_select.order(sort_by).page(@page).per_page(@per_page)
      set_link_header(@notification_setting)
    end

    def index_all
      head :forbidden and return if !current_user.super_user?
      if (params.key?(:resource_type) and params.key?(:event_type))
        notification_type = NotificationType.where(:resource_type => params[:resource_type], :event_type => params[:event_type]).first
        if !notification_type.nil?
          condition = {}
          condition[:notification_type_id] = notification_type.id
          if params.key?(:status)
            condition[:status] = params[:status].upcase
          end
          @notification_setting = NotificationSetting.where(condition)
        end
      else
        @notification_setting = NotificationSetting.all
      end

      unless @notification_setting
        return render json: []
      end
      
      @notification_setting = @notification_setting.in_dataplane(request_dataplane) if request_dataplane.present?
      @notification_setting = @notification_setting.page(@page).per_page(@per_page)
      @show_brief = true
      set_link_header(@notification_setting)
      render "index"
    end

    def show
      return if render_schema NotificationSetting
      @notification_setting = NotificationSetting.find(params[:id])
      authorize! :read, @notification_setting
    end

    def create
      input = (validate_body_json NotificationSetting).symbolize_keys
      @notification_setting = NotificationSetting.build_from_input(input, current_user, current_org)
      render "show"
    end

    def update
      input = (validate_body_json NotificationSetting).symbolize_keys

      @notification_setting = NotificationSetting.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @notification_setting.nil?
      authorize! :manage, @notification_setting

      @notification_setting.update_mutable!(request, current_user, input)
      render "show"
    end

    def destroy
      notification = NotificationSetting.find(params[:id])
      authorize! :manage, notification
      notification.destroy
      head :ok
    end

    def list
      head :forbidden and return if !current_user.super_user?

      @notification_setting = []
      notification_type_id = params[:notification_type_id]
      if (params[:notification_type_id].nil?)
        resource_type = params[:resource_type].present? ? [params[:resource_type].to_s.upcase, 'RESOURCE'] : nil
        notification_type = NotificationType.where(resource_type: resource_type, event_type: params[:event_type]).first
        if !notification_type.nil?
          notification_type_id = notification_type.id
        end
      end

      if !notification_type_id.nil?
        @notification_setting = get_valid_notification_setting(params, notification_type_id)
      end
      render "index"
    end

    def validate_settings_resource_type (resource_path_name)
      supported_resource_names = {
        "data_sources" => NotificationSetting::Resource_Types[:source],
        "data_sets" => NotificationSetting::Resource_Types[:dataset],
        "data_sinks" => NotificationSetting::Resource_Types[:sink]
      }
      supported_resource_names[resource_path_name]
    end

    def show_resource_settings

      resource_type = validate_settings_resource_type(params[:resource_type])
      if resource_type.nil?
        raise Api::V1::ApiError.new(:bad_request, "Invalid resource type in request path")
      end
      resource_id = params[:resource_id]
      notification_type = params[:notification_type_id]
      resource_model = ResourceAuthController::Resource_Types[resource_type]
      resource = resource_model.find(resource_id)
      authorize! :read, resource

      sort_order = (params[:sort_order] || 'ASC')
      sort_by = ((params[:sort_by] || 'priority')) + " #{sort_order}"
      filter_overridden_settings = params[:filter_overridden_settings].truthy?

      notification_select = NotificationSetting.select_waterfall(resource_type, resource_id, current_user.id, current_org&.id, notification_type, filter_overridden_settings)
      @notification_setting = notification_select.order(sort_by).page(@page).per_page(@per_page)
      set_link_header(@notification_setting)
      render "show_resource"
    end

    def get_valid_notification_setting(params, notification_type_id)
      res_type = [params[:resource_type], 'RESOURCE'].map{|s| "'#{s}'"}.join(',')
      condition_str = "(resource_id = #{params[:resource_id]} and notification_resource_type in (#{res_type}))"

      if params[:owner_id].is_a?(Array)
        condition_owner = "owner_id in ('" + params[:owner_id].join("','") + "')"
      else
        condition_owner = "owner_id = " + params[:owner_id].to_s
      end

      condition_str += " or (" + condition_owner + " and notification_resource_type = 'USER')"
      user = User.find_by_id(params[:owner_id])
      if !user.blank?
        org = user.default_org
        if !org.nil?
          condition_str += " or (org_id = " + org.id.to_s + " and notification_resource_type = 'ORG')"
        end
      end

      channel_priority = NotificationSetting.where("("+ condition_str +") and notification_type_id = " + notification_type_id.to_s)
                                  .group(:channel).maximum("priority")

      channel_priority_map = {}
      channel_priority.each do |channel, priority|
        channel_priority_map[channel] = priority
      end

      notification_settings = NotificationSetting.where("("+ condition_str +") and notification_type_id = " + notification_type_id.to_s)
                                 .order("priority")

      valid_settings = []
      notification_settings.each do |notification_setting|
        if channel_priority_map[notification_setting.channel] == notification_setting.priority
          valid_settings << notification_setting
        end
      end

      return valid_settings.nil? ? [] : valid_settings
    end

    def org_index
      org = Org.find(params[:org_id])
      authorize! :read, org

      sort_order = (params[:sort_order] || 'ASC')
      sort_by = ((params[:sort_by] || 'priority')) + " #{sort_order}"

      notification_select = NotificationSetting.where(:org_id => params[:org_id])
      @notification_setting = notification_select.order(sort_by).page(@page).per_page(@per_page)
      set_link_header(@notification_setting)
      render "index"
    end

    def org_create
      org = Org.find(params[:org_id])
      authorize! :manage, org
      input = (validate_body_json NotificationSetting).symbolize_keys
      input[:resource_type] = NotificationSetting::Resource_Types[:org]

      @notification_setting = NotificationSetting.build_from_input(input, current_user, org)
      render "show"
    end

    def org_update
      org = Org.find(params[:org_id])
      authorize! :manage, org
      input = (validate_body_json NotificationSetting).symbolize_keys

      @notification_setting = NotificationSetting.find_by_id(params[:notification_settings_id])
      raise Api::V1::ApiError.new(:not_found) if @notification_setting.nil?

      @notification_setting.update_mutable!(request, current_user, input)
      render "show"
    end

    def org_destroy
      org = Org.find(params[:org_id])
      authorize! :manage, org

      notification = NotificationSetting.find(params[:notification_settings_id])
      notification.destroy
      head :ok
    end

    def show_type_settings

      # return bad request if notification type ID is not an integer
      notification_type_id = params[:notification_type_id].to_i rescue nil

      if notification_type_id.nil?
        head :bad_request
      end

      notification_select = NotificationSetting.select_notification_type(notification_type_id, current_user.id, current_org.id)

      sort_order = (params[:sort_order] || 'ASC')
      sort_by = ((params[:sort_by] || 'priority')) + " #{sort_order}"

      @notification_setting = notification_select.order(sort_by).page(@page).per_page(@per_page)
      set_link_header(@notification_setting)
      render "show_type"
    end

  end
end
