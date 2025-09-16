module Api::V1  
  class ApiAuthConfigsController < Api::V1::ApiController
    
    skip_before_action :authenticate, only: [:index_client_config, :index_sign_on_options]
    before_action :validate_api_user, except: [:index_client_config, :index_sign_on_options]

    def validate_api_user
      raise Api::V1::ApiError.new(:method_not_allowed) if current_org.nil?
      authorize! :manage, current_org
    end

    # This is a legacy endpoint that's still being used on some private install environments.
    # We should remove this endpoint once we've migrated all of those environments to newer versions,
    # but for now we need to keep it around for backwards compatibility. (See NEX-4887)
    def index_client_config
      head :method_not_allowed and return if (ApiAuthConfig.count > 1)
      @api_auth_configs = ApiAuthConfig.all.page(@page).per_page(@per_page)
      set_link_header(@api_auth_configs)
      render "index_client_config"
    end

    def index      
      @api_auth_configs = ApiAuthConfig.where(:org => current_org).page(@page).per_page(@per_page)
      set_link_header(@api_auth_configs)
    end
    
    def info
      head :not_found and return if !current_user.super_user?
      re = Hash.new
      request.env.each do |k, v|
        re[k] = v if v.is_a?(String)
      end
      render json: {
        :base_url => ApiAuthConfig.generate_base_url(request),
        :request_ip => request.remote_ip,
        :reqest_env => re,
        :env => ENV
      }
    end

    def show
      return if render_schema ApiAuthConfig
      @api_auth_config = ApiAuthConfig.find_by_id(params[:id])
      @api_auth_config ||= ApiAuthConfig.find_by_uid(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @api_auth_config.nil?
      # Note, no need to call authorize! here, only org
      # admins have access. See validate_api_user aove.
    end
    
    def create
      input = (validate_body_json ApiAuthConfig).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @api_auth_config = ApiAuthConfig.build_from_input(api_user_info, input, request)
      render "show"
    end
    
    def update
      input = (validate_body_json ApiAuthConfig).symbolize_keys
      @api_auth_config = ApiAuthConfig.find_by_id(params[:id])
      @api_auth_config ||= ApiAuthConfig.find_by_uid(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @api_auth_config.nil?
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @api_auth_config)
      @api_auth_config.update_mutable!(api_user_info, input, request)
      render "show"
    end
    
    def destroy
      api_auth_config = ApiAuthConfig.find_by_id(params[:id])
      api_auth_config ||= ApiAuthConfig.find_by_uid(params[:id])
      raise Api::V1::ApiError.new(:not_found) if api_auth_config.nil?
      api_auth_config.destroy
      head :ok
    end

    def index_sign_on_options
      @sso_options = ApiAuthConfig.sso_options
      render "index_sign_on_options"
    end
  end
end


