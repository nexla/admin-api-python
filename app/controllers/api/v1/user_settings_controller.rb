module Api::V1  
  class UserSettingsController < Api::V1::ApiController      
    
    def index
      if (is_valid_search?)
        @user_settings = search.page(@page).per_page(@per_page)
      else
        @user_settings = UserSetting.where(:owner => current_user, :org => current_org)
          .page(@page).per_page(@per_page)
      end
      set_link_header(@user_settings)
    end
    
    def show
      return if render_schema UserSetting
      @user_setting = User.find(params[:id])
      authorize! :read, @user_setting
    end

    def create
      input = (validate_body_json UserSetting).symbolize_keys
      @user_setting = UserSetting.build_from_input(ApiUserInfo.new(current_user, current_org, input), input)
      render "show"
    end

    def update
      input = (validate_body_json UserSetting).symbolize_keys
      @user_setting = UserSetting.find(params[:id])
      authorize! :manage, @user_setting
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @user_setting)
      @user_setting.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      user_setting = UserSetting.find(params[:id])
      authorize! :manage, user_setting
      user_setting.destroy
      head :ok
    end

    protected

    def is_valid_search?
      return false if !current_org.has_admin_access?(current_user)
      return params[:all].truthy?
    end

    def search
      cnd = {}

      if (!request.raw_post.blank?)
        input = (validate_body_json UserSetting).symbolize_keys
        if (!input.key?(:user_settings_type))
          raise Api::V1::ApiError.new(:bad_request,
            "Required user_settings_type missing from input")
        end
        user_settings_type = UserSettingsType.find_by_name(input[:user_settings_type])
        cnd = { :user_settings_type_id => user_settings_type.id }
        if (input.key?(:primary_key_value))
          cnd[:primary_key_value] = input[:primary_key_value]
        end
      end

      return current_user.super_user? ? UserSetting.all.where(cnd) :
        UserSetting.where(:org => current_org).where(cnd)        
    end

  end
end