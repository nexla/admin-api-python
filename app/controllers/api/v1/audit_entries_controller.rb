require 'will_paginate/array'

module Api::V1  
  class AuditEntriesController < Api::V1::ApiController

    before_action only: [:audit_log, :flow_audit_log] do
      initialize_date_params(:current_and_previous_q)
    end 

    def audit_log
      model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      id_key = "#{model.name.underscore.singularize}_id".to_sym

      async_params = {
        resource_type: model.name,
        resource_id: params[id_key],
        interval: params[:interval],
        start_date: params[:from],
        end_date: params[:to]
      }
      return if process_async_request('GetAuditLogs', async_params)

      entries = (model == DataFlow) ? get_data_flow_audit_log(model, params) :
        get_standard_audit_log(model, params)
      @audit_entries = filter_and_sort_entries(entries)

      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    def flow_audit_log
      model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      raise Api::V1::ApiError.new(:bad_request) unless [FlowNode, DataSource, DataSet, DataSink].include?(model)


      async_params = {
        resource_type: model.name,
        resource_id: params[:resource_id] || params[:flow_node_id],
        interval: params[:interval],
        start_date: params[:from],
        end_date: params[:to],
        flow: true
      }
      return if process_async_request('GetAuditLogs', async_params)

      entries = get_flow_audit_log(model, params)
      @audit_entries = filter_and_sort_entries(entries)

      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    private

    def filter_and_sort_entries(entries)
      event_filter, negate = AuditEntryFilterValidator.validate_event_filter(params)
      if (!event_filter.blank?)
        entries = entries.select { |v|
          has = (v.event == event_filter)
          (negate ? !has : has)
        }
      end

      change_filter, negate = AuditEntryFilterValidator.validate_change_filter(params)
      if (!change_filter.blank?)
        if (change_filter.include?("acl"))
          entries = negate ?
                      entries.select { |v| !v.item_type.include?("AccessControl") } :
                      entries.select { |v| v.item_type.include?("AccessControl") }
        else
          entries = entries.select { |v|
            has = false
            v.object_changes.keys.each { |k| has = true if k.include?(change_filter) }
            (negate ? !has : has)
          }
        end
      end

      AuditEntry.sort_by_date(entries)
                .paginate(:page => @page, :per_page => @per_page)
    end

    def get_standard_audit_log (model, params)
      id = "#{model.name.underscore.singularize}_id".to_sym
      resource = model.find_by_id(params[id])

      if resource.present?
        authorize! :read, resource
        entries = resource.audit_log(@date_interval)
      else
        # Here we handle the case of fetching the audit log for
        # a deleted resource. We don't yet have a soft-delete
        # feature (as of api-2.7.1), so the resource is gone.
        # But its id lives on in the versions tables, in the
        # item_id column, if it ever existed.
        resource = model.new(id: params[id])
        entries = resource.audit_log(@date_interval)
        entry = entries.first
        raise ActiveRecord::RecordNotFound if !entry.present?
        if !entry.org.present?
          raise Api::V1::ApiError.new(:forbidden) unless
            (current_user.id == entry.owner_id) || current_user.super_user?
        elsif !entry.org.has_admin_access?(current_user)
          raise Api::V1::ApiError.new(:forbidden) unless
            (current_user.id == entry.owner_id) && (current_org == entry.org)
        end
      end

      if (model.respond_to?(:ac_versions_model) &&
        ConstantResolver.instance.versioned_models.include?(model.ac_model.name.underscore.to_sym))
        entries += model.ac_model.audit_log(
          @date_interval, {
            org_id: current_org.present? ? current_org.id : nil,
            resource_type: model.name,
            resource_id: resource.id
        })
      end
      
      return entries
    end

    def get_data_flow_audit_log (model, params)
      df_params = {
        :user => current_user,
        :org => current_org
      }
      case params[:resource_type]
      when :data_sink
        df_params[:data_sink_id] = params[:data_sink_id]
      when :data_source
        df_params[:data_source_id] = params[:data_source_id]
      else
        df_params[:data_set_id] = params[:data_flow_id]
      end
      df = DataFlow.new(df_params)
      authorize! :read, df
      return df.audit_log(@date_interval)
    end

    def get_flow_audit_log(model, params)
      resource_id = case params[:resource_type].to_sym
                    when :flow_node
                      params[:flow_node_id]
                    else
                      params[:resource_id]
                    end

      resource = model.find_by(id: resource_id)
      raise ActiveRecord::RecordNotFound unless resource.present?

      authorize! :read, resource

      origin_node = resource.origin_node
      raise Api::V1::ApiError.new(:bad_request) unless origin_node.present?

      entries = []
      api_user_info = ApiUserInfo.new(current_user, current_org)
      resources = origin_node.resources(api_user_info)
      FlowNode::Accessible_Models.each do |m|
        resource_type = m.name.pluralize.underscore.to_sym
        resources[resource_type].each do |r|
          entries.concat(r.audit_log(@date_interval))
        end
      end

      entries.concat(origin_node.audit_log_traverse(@date_interval))
    end

    def validate_change_filter (params)
      return nil, false if params[:change_filter].blank?

      valid = [
        "status", "acl", "schema", "samples", "owner",
        "org", "transform", "code", "config", "copied"
      ]
      negate = false

      change_filter = params[:change_filter].downcase
      if (change_filter.first == "!")
        change_filter = change_filter[1..-1]
        negate = true
      end
      
      raise Api::V1::ApiError.new(:bad_request, "Invalid changed filter") if !valid.include?(change_filter)
      change_filter = "code" if (change_filter == "transform")

      return change_filter, negate
    end
  end
end
