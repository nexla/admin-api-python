module RequestFilters
    extend ActiveSupport::Concern
    AND_CLAUSE = 'AND'.freeze
    OR_CLAUSE = 'OR'.freeze

    CLAUSES = [AND_CLAUSE, OR_CLAUSE].freeze
    
    REQUEST_FILTERS = {
      like_filters: [:name],
      equal_filters: [:id, :status, :credentials_type, :org_id, :data_source_id, :source_schema_id, :auth_template_id, :vendor_endpoint_id, :resource_id, :level, :mode, :ai_function_type],
      custom_filters: {
        auth_template_name: Proc.new do |model|
          parse_condition("auth_template_id", AuthTemplate.where(name: params[:auth_template_name]).ids) if model == AuthParameter || model == DataCredentials
        end,
        parent_data_set_id: Proc.new do |model|
          # NOTICE: parent data sets are not filtered out by access
          parse_condition("id", DataSet.where(parent_data_set_id: params[:parent_data_set_id]).pluck(:id)) unless params[:source].truthy?
        end,
        source: Proc.new do |model|
          (params[:source].truthy? ? "data_source_id is not NULL" : "data_source_id is NULL") if model == DataSet
        end,
        vendor_endpoint_name: Proc.new do |model|
          # NOTICE: on DataSets will result error 500
          if model == ResourceParameter
            parse_condition("vendor_endpoint_id", VendorEndpoint.where(name: params[:vendor_endpoint_name]).ids)
          else
            parse_condition("vendor_endpoint_id", VendorEndpoint.where("name like '%#{params[:vendor_endpoint_name]}%'").ids)
          end
        end,
        vendor_name: Proc.new do |model|
          if model == ResourceParameter
            parse_condition("vendor_endpoint_id", VendorEndpoint.joins(:vendor).where("vendors.name = '#{params[:vendor_name]}'").ids)
          elsif model == AuthParameter || model == VendorEndpoint || model == AuthTemplate
            parse_condition("vendor_id", Vendor.where(name: params[:vendor_name]).ids)
          elsif model == DataSource || model == DataSink
            parse_condition("vendor_endpoint_id", VendorEndpoint.joins(:vendor).where("vendors.name like '%#{params[:vendor_name]}%'").ids)
          elsif model == DataCredentials
            parse_condition("vendor_id", Vendor.where("name like '%#{params[:vendor_name]}%'").ids)
          end
        end,
        validity: Proc.new do |model|
          (params[:validity].to_s.downcase == "valid") ? "verified_at is NOT NULL" : "verified_at is NULL" if model == DataCredentials
        end,
        vendor_id: Proc.new do |model|
          if model == ResourceParameter
            parse_condition("vendor_endpoint_id", VendorEndpoint.where(vendor_id: params[:vendor_id]).ids)
          else
            parse_condition("vendor_id", params[:vendor_id])
          end
        end,
        resource_type: Proc.new do |model|
          field = params[:resource_type] == "SOURCE" ? "source_template" : "sink_template"
          if model == VendorEndpoint
            "#{field} is not NULL"
          else
            parse_condition("resource_type", params[:resource_type])
          end
        end,
        global: Proc.new do |model|
          parse_condition("global", params[:global].truthy? ? 1 : 0)
        end,
        tags: Proc.new do |model|
          tagged = ResourceTagging.search_by_tags(model, Hash["tags", params[:tags]], current_user, request_access_role, current_org)
          parse_condition("id", tagged.nil? ? [] : tagged.ids)
        end
      }
    }
  
    def add_request_filters(scope, model)
      conditions = [
        REQUEST_FILTERS[:like_filters].map { |q|  parse_condition(q, params[q],"contains") if params.key?(q) },
        REQUEST_FILTERS[:equal_filters].map { |q| parse_condition(q, params[q].is_a?(Array) ? params[q] : params[q].to_s.downcase) if params.key?(q) },
        REQUEST_FILTERS[:custom_filters].map { |q,proc| self.instance_exec(model, &proc) if params.key?(q) }
      ]

      # Note, here we convert the first applicable connector-type filter
      # to the native column type, :connector_type. We ignore caller errors
      # like &sink_type=<>&source_type=<>. See NEX-11986.
      type_filter = params.slice(:connector_type, :source_type, :sink_type).values.first
      conditions << parse_condition(:connector_type, type_filter.to_s.downcase.split(',')) if type_filter.present?

      conditions = conditions.flatten.compact
      scope = scope.where(conditions.join(" #{params[:condition_type] == "or" ? "or" : "and"} "))

      if params.key?(:flow_type)
        scope = scope.joins(:origin_node).where(origin_node: { flow_type: params[:flow_type] }).select(model.table_name + ".*")
      end
      if params.key?(:nexset_api_compatible) || params.key?(:sync_api_compatible)
        flag_value = params.key?(:nexset_api_compatible) ? params[:nexset_api_compatible] : params[:sync_api_compatible]
        scope = scope.joins(:origin_node).where(origin_node: { nexset_api_compatible: flag_value}).select(model.table_name + ".*")
      end
      if params[:not_rag].truthy?
        scope = scope.joins(:origin_node).where.not(origin_node: { flow_type: FlowNode::Flow_Types[:rag]}).select(model.table_name + ".*")
      end
      sort_by(scope)
    end

    def sort_by(resource)
      model = resource.klass

      if model.name == 'Notification'
        default_sort_by = "timestamp"
      else
        default_sort_by = "created_at"
      end
      sort_by = params[:sort_by] || default_sort_by
      sort_by = 'timestamp' if sort_by == 'ts'
      sort_order = params[:sort_order] || "DESC"

      if model.name == 'FlowNode' && sort_by == 'run_id'
        resource
          .left_outer_joins(:data_source)
          .order("ISNULL(data_sources.last_run_id), data_sources.last_run_id #{sort_order}")
          .select(model.table_name + ".*")
      else
        sort_by = 'last_run_id' if sort_by.downcase == 'run_id'

        resource.order("#{resource.table_name}.#{sort_by} #{sort_order}")
      end
    end

    def parse_condition(column, value, operator = 'eq')
      case operator
      when 'contains'
        "#{column} like '%#{value}%' "
      else
        return "#{column} in ('#{value.join("','")}')" if value.is_a?(Array)
        "#{column} = '#{value}' "
      end
    end

end
