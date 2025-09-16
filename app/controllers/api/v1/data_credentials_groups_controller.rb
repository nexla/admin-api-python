module Api::V1
  class DataCredentialsGroupsController < Api::V1::ApiController
    include AccessorsConcern

    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @data_credentials_groups = add_request_filters(DataCredentialsGroup.accessible_by_user(current_user, current_org, options), DataCredentialsGroup)
      @data_credentials_groups = @data_credentials_groups.page(@page).per_page(@per_page)
      set_link_header(@data_credentials_groups)
    end

    def show
      @data_credentials_group = DataCredentialsGroup.find(params[:id])
      authorize! :read, @data_credentials_group

      render :show
    end

    def index_credentials
      @data_credentials_group = DataCredentialsGroup.find(params[:data_credentials_group_id])
      authorize! :read, @data_credentials_group

      @data_credentials = @data_credentials_group.data_credentials
      @data_credentials = @data_credentials.page(@page).per_page(@per_page)
      set_link_header(@data_credentials)

      render 'api/v1/data_credentials/index'
    end

    def create
      input = (validate_body_json DataCredentialsGroup).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_credentials_group = DataCredentialsGroup.build_from_input(api_user_info, input)
      render :show
    end

    def update
      input = (validate_body_json DataCredentialsGroup).symbolize_keys
      @data_credentials_group = DataCredentialsGroup.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_credentials_group.nil?
      authorize! :manage, @data_credentials_group

      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_credentials_group)
      @data_credentials_group.update_mutable!(api_user_info, input)
      render :show
    end

    def destroy
      @data_credentials_group = DataCredentialsGroup.find(params[:id])
      authorize! :manage, @data_credentials_group

      if @data_credentials_group.destroy
        head :no_content
      else
        raise Api::V1::ApiError.new(:method_not_allowed, message: @data_credentials_group.errors.messages[:base].join("\n"))
      end
    end

    def remove_credentials
      @data_credentials_group = DataCredentialsGroup.find(params[:data_credentials_group_id])
      authorize! :manage, @data_credentials_group

      credentials_ids = params[:data_credentials]
      api_user_info = ApiUserInfo.new(current_user, current_org)
      @data_credentials_group.remove_credentials(api_user_info, credentials_ids)

      @data_credentials = @data_credentials_group.data_credentials
      @data_credentials = @data_credentials.page(@page).per_page(@per_page)
      set_link_header(@data_credentials)

      render 'api/v1/data_credentials/index'
    end
  end
end
