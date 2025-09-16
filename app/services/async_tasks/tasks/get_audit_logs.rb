module AsyncTasks::Tasks
  class GetAuditLogs < AsyncTasks::Tasks::Base

    extend Memoist

    AUDIT_LOGS_VIEW_PATH = "api/v1/audit_entries/audit_log".freeze

    def run
      file_key = nil
      Tempfile.create do |file|
        Rails.logger.info "[AuditLog AsyncTask] Rendering to file"
        render_to_file(file)
        Rails.logger.info "[AuditLog AsyncTask] Rendering to file done"

        file.rewind
        Rails.logger.info "[AuditLog AsyncTask] Uploading"
        file_key = upload_file(file)

        Rails.logger.info "[AuditLog AsyncTask] Store Download URL"
        url = S3Service.new.get_presigned_url(AsyncTasks::Manager.results_s3_bucket, file_key)
        task.update(result_url: url)
      end

      Rails.logger.info "[AuditLog AsyncTask] Done, exiting"
      { file_key: file_key, storage: 's3', bucket: AsyncTasks::Manager.results_s3_bucket }
    end

    def check_preconditions
      if args[:org_id].present? && !Org.where(id: args[:org_id]).exists?
        raise Api::V1::ApiError.new(:bad_request, "Couldn't find org")
      end

      if org && !org.has_collaborator_access?(task_owner)
        raise Api::V1::ApiError.new(:forbidden)
      end

      if resource && !resource.has_collaborator_access?(task_owner)
        raise Api::V1::ApiError.new(:forbidden)
      end

      if args[:resource_id].blank? && args[:resource_type].present?
        unless ConstantResolver.instance.versioned_models.include?( args[:resource_type].to_s.underscore.to_sym)
          raise Api::V1::ApiError.new(:bad_request, "Invalid resource_type")
        end
        org = Org.find(args[:org_id] || task.org_id)
        raise Api::V1::ApiError.new(:forbidden, "No read access to Org ##{org.id}") unless org.has_collaborator_access?(task_owner)
      end

      if args[:start_date].present?
        begin
          Date.parse(args[:start_date])
        rescue ArgumentError
          raise Api::V1::ApiError.new(:bad_request, "Invalid period_start date")
        end
      end

      if args[:end_date].present?
        begin
          Date.parse(args[:end_date])
        rescue ArgumentError
          raise Api::V1::ApiError.new(:bad_request, "Invalid period_end date")
        end
      end

      AuditEntryFilterValidator.validate_change_filter(args) if args[:event_filter].present?
      AuditEntryFilterValidator.validate_change_filter(args) if args[:change_filter].present?
    end

    def explain_arguments
      event_filters = AuditEntryFilterValidator::VALID_EVENT_TYPES.map{|v| "'#{v}'"}.join(", ")
      change_filters = AuditEntryFilterValidator::VALID_CHANGE_FILTERS.map{|v| "'#{v}'"}.join(", ")

      {
        user_id: "ID of the user to get audit logs for (optional). If not provided, the audit logs will be fetched unfiltered by user.",
        org_id: "ID of the organization to get audit logs for (optional). If not provided, the audit logs will be fetched for current org.",
        resource_type: "Type of the resource to get audit logs for (optional). If resource is not provided, the audit logs will be fetched for the organization.",
        resource_id: "ID of the resource to get audit logs for (optional). If resource is not provided, the audit logs will be fetched for the organization.",
        start_date: "Start date of the audit logs (optional). If not provided, the audit logs will be fetched from the beginning of time.",
        end_date: "End date of the audit logs (optional). If not provided, the audit logs will be fetched up to the current time.",
        interval: "Interval of audit logs (optional). By default is set to 'lifetime'.",
        expand: "Whether to expand the results (optional). By default is set to 'false'.",
        event_filter: "Filter the audit logs by event type (optional). Values can be #{event_filters}.",
        change_filter: "Filter the audit logs by change type (optional). Values can be #{change_filters}.",
        flow: "Whether to fetch audit logs for data flows (optional). By default is set to 'false'."
      }
    end

    def provides_download_link?
      true
    end

    private

    memoize
    def org
      args[:org_id].presence && Org.find_by(id: args[:org_id])
    end

    memoize
    def resource
      if args[:resource_type].present? && args[:resource_id].present?
        return args[:resource_type].constantize.find_by(id: args[:resource_id])
      end
      nil
    end

    def render_to_file(file)
      if args[:interval].present? || args[:start_date].present? || args[:end_date].present?
        date_interval = DateInterval.new(args[:interval], args[:from], args[:to])
      else
        date_interval = DateInterval.new("lifetime")
      end

      if args[:flow].truthy?
        audit_entries = get_flow_audit_log(date_interval)
      else
        audit_entries = get_audit_entries(date_interval)
      end
      assigns = {
        api_root: "api/v1/",
        audit_entries: audit_entries,
        date_interval: date_interval,
        expand: args[:expand].truthy?
      }
      Rails.logger.info "[AuditLog AsyncTask] > Rendering view to memory"
      content = ApplicationController.render(template: AUDIT_LOGS_VIEW_PATH, formats: [:json], assigns: assigns)

      Rails.logger.info "[AuditLog AsyncTask] > Writing content to file"
      file.write(content)
    end

    def get_flow_audit_log(date_interval)
      origin_node = resource.origin_node

      entries = []
      api_user_info = ApiUserInfo.new(task_owner, task.org)
      resources = origin_node.resources(api_user_info)

      FlowNode::Accessible_Models.each do |m|
        resource_type = m.name.pluralize.underscore.to_sym
        resources[resource_type].each do |r|
          entries.concat(r.audit_log(date_interval))
        end
      end

      entries.concat(origin_node.audit_log_traverse(date_interval))
      entries
    end

    def get_audit_entries(date_interval)
      filter, negative_filter = create_filter
      if args[:user_id].present?
        filter << { owner_id: @user.id }
      end

      if args[:resource_type].present? && args[:resource_id].present?
        resource = args[:resource_type].classify.constantize.find_by(id: args[:resource_id])
        if resource
          audit_entries = resource.audit_log(date_interval, filter, negative_filter)
        else
          # Deleted record
          resource = model.new(id: arg[id])
          audit_entries = resource.audit_log(date_interval, filter, negative_filter)
        end
        model = resource.class

        if model.respond_to?(:ac_versions_model) && ConstantResolver.instance.versioned_models.include?(model.ac_model.name.underscore.to_sym)
          audit_entries += model.ac_model.audit_log(
            date_interval, {
            org_id: org.present? ? org.id : nil,
            resource_type: model.name,
            resource_id: resource.id
          })
        end
      elsif args[:resource_type].present?
        org = Org.find( args[:org_id] || task.org_id )
        audit_entries = AuditEntry.new(org).log_for_resource(args[:resource_type], date_interval, filter, negative_filter)
      else
        org = Org.find( args[:org_id] || task.org_id )
        audit_entries = AuditEntry.new(org).all(date_interval, filter, negative_filter)
      end
      audit_entries
    end

    def create_filter
      filter = []
      negative_filter = []
      event_filter, negate = AuditEntryFilterValidator.validate_event_filter(args)
      if event_filter.present?
        if negate
          negative_filter << { event: event_filter }
        else
          filter << { event: event_filter }
        end
      end

      change_filter, negate = AuditEntryFilterValidator.validate_change_filter(args)
      if change_filter.present?
        if (change_filter.include?("acl"))
          if negate
            negative_filter << "item_type not like '%AccessControl%'"
          else
            filter << "item_type like '%AccessControl%'"
          end
        else
          if negate
            negative_filter << "object_changes like '%#{change_filter}%'"
          else
            filter << "object_changes like '%#{change_filter}%'"
          end
        end
      end

      [filter, negative_filter]
    end

    def upload_file(file)
      file_name = "audit_logs_#{Time.now.to_i}.csv"
      S3Service.new.upload_file(file, AsyncTasks::Manager.results_s3_bucket, file_name)
      file_name
    end

  end
end