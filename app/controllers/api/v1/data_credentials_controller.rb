module Api::V1
  class DataCredentialsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include AccessorsConcern
    include ControlEventConcern

    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      credentials_type = params.delete(:credentials_type)
      @data_credentials = add_request_filters(current_user.data_credentials(current_org, options), DataCredentials)

      # NEX-9674 github type credentials are not currently probe-able,
      # so we filter them out of listing responses. They can still be
      # managed manually for Github integrations. This filter can be
      # removed when Github integration is fully built out as a feature.
      @data_credentials = @data_credentials.where.not(connector_type: DataSource.connector_types[:github])
      # NEX-9277 dynamic lookup sinks create new nameless data credentials that are confusing
      # to customers, so we filter them out here temporarily.
      @data_credentials = @data_credentials.where.not(name: nil, description: nil)

      if credentials_type.present?
        @data_credentials = @data_credentials.by_credentials_type(credentials_type)
      end

      @data_credentials = @data_credentials.page(@page).per_page(@per_page)
      set_link_header(@data_credentials)

      load_tags(@access_roles)
    end

    def index_all
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      @data_credentials = add_request_filters(DataCredentials.jit_preload, DataCredentials)
      @data_credentials = @data_credentials.in_dataplane(request_dataplane) if request_dataplane.present?
      @data_credentials = @data_credentials.page(@page).per_page(@per_page)
      set_link_header(@data_credentials)
      render "optimized"
    end

    def credentials_schema
      source_type = DataCredentials.validate_connector_type(params[:source_type])
      result = ProbeService.new.get_credentials_schema(source_type, current_org)
      render json: result, status: result[:status]
    end

    def show
      return if render_schema DataCredentials
      @data_credentials = DataCredentials.find(params[:id])
      authorize! :read, @data_credentials
    end

    def create
      input = (validate_body_json DataCredentials).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_credentials = DataCredentials.new
      @data_credentials.set_defaults(api_user_info.input_owner, api_user_info.input_org)
      @data_credentials.update_mutable!(api_user_info, input, request)
      ResourceTagging.after_create_tagging(@data_credentials, input, current_user)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_data_credentials = DataCredentials.find(params[:data_credential_id])
      authorize! :manage, copied_data_credentials
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_data_credentials)
      @data_credentials = copied_data_credentials.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json DataCredentials).symbolize_keys
      @data_credentials = DataCredentials.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_credentials.nil?
      authorize! :manage, @data_credentials
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_credentials)
      @data_credentials.update_mutable!(api_user_info, input, request)
      render "show"
    end

    def destroy
      @data_credentials = DataCredentials.find(params[:id])
      authorize! :manage, @data_credentials
      if @data_credentials.destroy
        head :ok
      else
        raise Api::V1::ApiError.new(:method_not_allowed, message: @data_credentials.errors.messages[:base].join("\n"))
      end
    end

    def refresh
      @data_credentials = DataCredentials.find(params[:data_credential_id])
      authorize! :manage, @data_credentials
      @data_credentials.refresh
      render "show"
    end

    def probe_authenticate
      return if process_async_request("CallProbe", { type: "DataCredentials", id: params[:data_credential_id], action: "authenticate" })

      data_credentials = DataCredentials.find(params[:data_credential_id])
      authorize! :read, data_credentials

      result = ProbeService.new(data_credentials).authenticate
      data_credentials.verified_status = "#{result[:status]}" + " #{result[:message]}"
      data_credentials.save!

      if result[:status] == :ok
        head result[:status]
        return
      end

      render :json => result, :status => result[:status], :message => result[:message]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_credentials = ResourceTagging.search_by_tags(DataCredentials, input, current_user, request_access_role, current_org)
      set_link_header(@data_credentials)
      render "index"
    end

    def db_data_types
      source_type = DataCredentials.validate_connector_type(params[:source_type])
      result = ProbeService.new.get_db_data_types(source_type, current_org)
      render :json => result, :status => result[:status]
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      @data_credentials = current_user.data_credentials(current_org, { access_role: request_access_role, access_roles: @access_roles })
      @data_credentials = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataCredentials, params[:filters], @data_credentials, sort_opts: sort_opts).call
      @data_credentials = @data_credentials.page(@page).per_page(@per_page)
      set_link_header(@data_credentials)

      load_tags(@access_roles)
      render :index
    end

    def usage
      @data_credentials = DataCredentials.find(params[:data_credential_id])
      authorize! :read, @data_credentials
      @attrs = [ :id, :flow_node_id, :origin_node_id, :owner_id, :org_id, :name, :status ]
      @data_sources = DataSource.where(data_credentials_id: @data_credentials.id).select(@attrs)
      @data_sinks = DataSink.where(data_credentials_id: @data_credentials.id).select(@attrs)
      @quarantine_attrs = [:id, :owner_id, :org_id]
      @quarantine_settings = @data_credentials.quarantine_settings
      render :usage
     end

    def migrate_iceberg
      data_credentials = DataCredentials.find(params[:data_credential_id])
      authorize! :read, data_credentials

      raise Api::V1::ApiError.new(:method_not_allowed) if data_credentials.connector_type != DataSource.connector_types[:s3]

      result = ProbeService.new(data_credentials).migrate_iceberg(
        params[:source_snapshot_path],
        params[:destination_iceberg_warehouse], 
        params[:destination_snapshot_path]
      )

      render :json => result, :status => result[:status]
    end
  end
end
