module Api::V1
  class ResourceAuthController < Api::V1::ApiController
    skip_before_action :authenticate
    before_action :verify_api_key_or_token

    Resource_Types = {
      'ORG'               => Org,
      'USER'              => User,
      'SOURCE'            => DataSource,
      'DATASET'           => DataSet,
      'SINK'              => DataSink,
      'CREDENTIALS'       => DataCredentials,
      'MAP'               => DataMap,
      'CUSTOM_DATA_FLOW'  => CustomDataFlow,
      'DATA_FLOW'         => DataFlow,
      'FLOW'              => DataSource
    }

    def get_resource_model (type_str)
      return nil if !type_str.is_a?(String)
      return Resource_Types[type_str.upcase]
    end

    def get_resource_and_access_mode
      resource_model = get_resource_model(params[:resource_type])
      resource_id = params[:resource_id]
      if (resource_model.nil? || resource_id.nil?)
        raise Api::V1::ApiError.new(:bad_request, "Missing or unknown resource type")
      end

      if params[:resource_type].upcase == 'FLOW'
        origin_node = FlowNode.find(resource_id).origin_node
        resource_id = origin_node.data_source_id
        raise Api::V1::ApiError.new(:bad_request, "Missing or unknown resource id") if resource_id.blank?
      end

      resource = resource_model.find(resource_id)
      if (params[:access_mode].is_a?(String) && (params[:access_mode].downcase == "manage"))
        access_mode = :manage
      else
        access_mode = :read
      end
      return resource, access_mode
    end

    def verify_api_key_or_token
      @resource, @access_mode = get_resource_and_access_mode
      1.times do
        break if !valid_request_origin?

        auth = request.headers['Authorization']
        if (auth.blank? && params.key?(:api_key))
          auth = "Basic #{params[:api_key]}"
        end
        break if auth.blank?

        auth_parts = auth.split
        break if ((auth_parts.size < 2) || !auth_parts.first.is_a?(String))

        case auth_parts.first
        when "Bearer"
          return verify_token(*auth_parts, request, logger)
        when "Basic"
          return verify_api_key(auth_parts.second, @resource)
        else
          break
        end
      end

      return false
    end

    def verify_api_key (api_key, resource)
      # Note, the caller of this method MUST authorize access
      # to the resource if 'true' is returned. This method does
      # NOT authorize, it just finds a matching api key entry,
      # if any, and sets @api_user and @api_org from it.
      iak = Cluster.get_infrastructure_access_key(request_dataplane&.dataplane_key)
      if (iak.present? && iak.matches?(api_key))
        if iak.user_id.present?
          u = User.find_by_id(iak.user_id)
          o = Org.find_by_id(iak.org_id)
          return false if u.nil? || o.nil?
        else
          u = User.nexla_backend_admin
          o = Org.get_nexla_admin_org
        end
        u&.infrastructure_user = true
        Current.set_user(u)
        Current.set_org(o)

        current_user.org = current_org
      else
        api_key_entry = nil

        if (resource.respond_to?(:api_keys))
          api_key_entry = @resource.api_keys.where(:api_key => api_key).first
        end

        if (api_key_entry.nil?)
          api_key_entry = UsersApiKey.where(:api_key => api_key, :scope => "all").first
        end

        return false if api_key_entry.nil?
        return false if !api_key_entry.active?

        Current.set_user(api_key_entry.owner)
        Current.set_org(api_key_entry.org)
        current_user.org = current_org
        audit_api_key api_key_entry unless api_key_entry.is_a?(ServiceKey)
      end

      return true
    end

    def resource_authorize
      if (request_dataplane.present? && (request_dataplane.id != @resource.org&.cluster&.id))
        raise Api::V1::ApiError.new(:unauthorized, "Invalid dataplane for resource")
      end

      authorize! @access_mode, @resource
      if params[:provisioning].truthy? && @resource.is_a?(DataSet)
        # Note, this is an optimization for provisioning Nexset-API flows
        # during the first /resource_authorize call.
        # It is the caller's responsibility to ensure the nexset is
        # nexset-api (sync-api) compatible.
        builder = Flows::Builders::RenderBuilder.new(nil, [@resource.flow_node])
        @resources = builder.build_provisioning(@resource.flow_node)
        render "api/v1/flows/show_provisioning"
      else
        head :ok
      end
    end

  end
end
