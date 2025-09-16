module Api::V1  
  class NotificationsController < Api::V1::ApiController
    include Api::V1::ApiKeyAuth

    skip_before_action :authenticate, only: [:index]
    before_action only: [:index] do 
      verify_authentication(UsersApiKey::Nexla_Monitor_Scopes)
    end

    def index
      notification_select = nil
      if (!params[:resource_id].blank? && !params[:resource_type].blank?)
        resource = nil
        resource_type = params[:resource_type].upcase
        case resource_type
        when "SOURCE"
          resource = DataSource.find(params[:resource_id])
        when "DATASET"
          resource = DataSet.find(params[:resource_id])
        when "SINK"
          resource = DataSink.find(params[:resource_id])
        end
        if (!resource.nil?)
          authorize! :read, resource
          notification_select = Notification.where(:resource_id => params[:resource_id],
            :resource_type => resource_type)
        end
        # Remove resource id and type so they aren't applied
        # again in the add_request_filters() call below:
        params.delete(:resource_id)
        params.delete(:resource_type)
      end

      if notification_select.nil?
        notification_select = Notification.where(
          :owner_id => current_user.id,
          :org_id => (current_org&.id)
        )
      end

      if (params.key?(:read))
        if (params[:read].truthy?)
          notification_select = notification_select.where.not(read_at: nil)
        else
          notification_select = notification_select.where(read_at: nil)
        end
      end

      if params[:from].present?
        from = Float(params[:from]) rescue nil
        raise Api::V1::ApiError.new(:bad_request, "Invalid 'from' date format") unless from
        from = DateInterval.unix_to_db_datetime_str(params[:from]).gsub(" ", "T")
        notification_select = notification_select.where("timestamp >= ?", from)
      end

      if params[:to].present?
        to = Float(params[:to]) rescue nil
        raise Api::V1::ApiError.new(:bad_request, "Invalid 'to' date format") unless to
        to = DateInterval.unix_to_db_datetime_str(params[:to]).gsub(" ", "T")
        notification_select = notification_select.where("timestamp <= ?", to)
      end

      notification_select = notification_select.jit_preload
      @notifications = add_request_filters(notification_select, Notification).page(@page).per_page(@per_page)
      set_link_header(@notifications)
    end
    
    def show
      return if render_schema Notification
      @notification = Notification.find(params[:id])
      authorize! :read, @notification
    end
    
    def create
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      input = (validate_body_json Notification).symbolize_keys
      @notification = Notification.build_from_input(current_user, current_org, input)
      render "show"
    end
    
    def update
      head :method_not_allowed
    end
    
    def destroy
      notification_ids = []
      destroy_all = false

      if params[:id] == "all"
        return if process_async_request("BulkDeleteNotifications", params)

        destroy_all = true
      elsif !params[:id].blank?
        notification_ids << params[:id].to_i
      end

      if !destroy_all && !request.raw_post.empty?
        body = MultiJson.load(request.raw_post)
        body = body["ids"] || body["notification_ids"] if body.is_a?(Hash)
        body.each { |nid| notification_ids << nid if nid.is_a?(Integer) } if body.is_a?(Array)
      end

      raise Api::V1::ApiError.new(:not_found) if !destroy_all && notification_ids.empty?
      notifications = []

      if destroy_all
        notifications = current_user.notifications(:owner, current_org)
      else
        notification_ids = notification_ids.uniq
        notification_ids.each do |nid|
          n = Notification.where(:id => nid)[0]
          next if n.nil?
          authorize! :manage, n
          notifications << n
        end
      end

      head :not_found and return if notifications.empty?

      if !destroy_all
        notifications.each(&:destroy)
      else
        notifications.delete_all
      end
      head :ok
    end

    def mark_read
      notification_ids = []
      mark_all = false

      if params[:notification_id] == "all"
        mark_all = true
      elsif !params[:notification_id].blank?
        notification_ids << params[:notification_id].to_i
      end

      if !mark_all && !request.raw_post.empty?
        body = MultiJson.load(request.raw_post)
        body = body["ids"] || body["notification_ids"] if body.is_a?(Hash)
        body.each { |nid| notification_ids << nid if nid.is_a?(Integer) } if body.is_a?(Array)
      end

      raise Api::V1::ApiError.new(:not_found) if !mark_all && notification_ids.empty?
      @notifications = []

      read_at = params[:read] ? Time.now : nil

      if mark_all
        return if process_async_request("BulkMarkAsReadNotifications", params)

        org_id = current_org&.id
        notifications = Notification.where(:owner_id => current_user, :org_id => org_id)
        notifications.update_all(:read_at => read_at)
      else
        notification_ids = notification_ids.uniq
        notification_ids.each do |nid|
          n = Notification.where(:id => nid)[0]
          next if n.nil?
          authorize! :manage, n
          @notifications << n
        end
        notification_ids_list = (@notifications.collect(&:id))
        Notification.where(:id => notification_ids_list).update_all(:read_at => read_at)
      end

      head :ok
    end

    def publish
      head :forbidden and return if !current_user.infrastructure_or_super_user?

      input = params.permit(%w[resource_id resource_type event_time_millis event_source event_type context]).to_h
      result = NotificationService.new.publish_raw(current_org, input)
      render :json => result, :status => result[:status]
    end

    def count
      notification_select = Notification.where(
        :owner_id => current_user.id, 
        :org_id => (current_org&.id)
      )
      if (params.key?(:read))
        if (params[:read].truthy?)
          notification_select = notification_select.
              where.not(:read_at => nil)
        else
          notification_select = notification_select.
              where(:read_at => nil)
        end
      end
      result = {}
      result[:count] = notification_select.count

      render :json => result, :status => :ok
    end

  end
end


