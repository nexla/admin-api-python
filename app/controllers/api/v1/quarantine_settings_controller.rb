module Api::V1
  class QuarantineSettingsController < Api::V1::ApiController

    before_action do
      raise Api::V1::ApiError.new(:bad_request, "Org context is required but not set") if current_org.nil?
    end

    def index_all
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      @quarantine_setting = add_request_filters(QuarantineSetting, QuarantineSetting)
      @quarantine_setting = @quarantine_setting.in_dataplane(request_dataplane) if request_dataplane.present?
      @quarantine_setting = @quarantine_setting.page(@page).per_page(@per_page)

      set_link_header(@quarantine_setting)
      render "index"
    end

    def index
      quarantine_select = current_user.quarantine_settings(request_access_role, current_org)
      @quarantine_setting = add_request_filters(quarantine_select, QuarantineSetting)
        .page(@page).per_page(@per_page)
      set_link_header(@quarantine_setting)
    end

    def show
      return if render_schema QuarantineSetting
      resource_type, resource_id = get_resource_info

      @quarantine_setting = QuarantineSetting.where(
        owner_id: current_user.id,
        org_id: current_org&.id,
        resource_type: resource_type,
        resource_id: resource_id
      ).first

      raise Api::V1::ApiError.new(:not_found) if @quarantine_setting.nil?
    end

    def create
      input = (validate_body_json QuarantineSetting).symbolize_keys
      resource_type, resource_id = get_resource_info(input)
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      input[:resource_type] = resource_type
      input[:resource_id] = resource_id

      @quarantine_setting = QuarantineSetting.find_by(
        owner_id: current_user.id,
        org_id: current_org&.id,
        resource_type: resource_type,
        resource_id: resource_id
      )

      if @quarantine_setting.present?
        # Note, we allow POST to behave like PUT if there is already
        # an entry for the given resource type/id combination.
        @quarantine_setting.update_mutable!(api_user_info, input)
      else
        @quarantine_setting = QuarantineSetting.build_from_input(api_user_info, input)
      end

      render "show"
    end

    def update
      input = (validate_body_json QuarantineSetting).symbolize_keys
      resource_type, resource_id = get_resource_info(input)
      api_user_info = ApiUserInfo.new(current_user, current_org, input)

      @quarantine_setting = QuarantineSetting.where(
        owner_id: current_user.id,
        org_id: current_org&.id,
        resource_type: resource_type, 
        resource_id: resource_id
      ).first

      raise Api::V1::ApiError.new(:not_found) if @quarantine_setting.nil?
      @quarantine_setting.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      resource_type, resource_id = get_resource_info
      quarantine_setting = QuarantineSetting.where(
        owner_id: current_user.id,
        org_id: current_org&.id,
        resource_type: resource_type,
        resource_id: resource_id
      ).first
      raise Api::V1::ApiError.new(:not_found) if quarantine_setting.nil?
      quarantine_setting.destroy
      head :ok
    end

    def get_resource_info (input = nil)
      model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      model_sym = model.name.underscore.singularize.to_sym
      key = "#{model_sym}_id".to_sym

      resource_id = params[key].to_i
      resource_type = QuarantineSetting::Resource_Types[model_sym]

      validate_user_input(resource_id, input) if (model_sym == :user)
      return resource_type, resource_id
    end

    def validate_user_input (user_id, input)
      return if input.blank?

      if input[:resource_id].present? && (input[:resource_id] != user_id)
        raise Api::V1::ApiError.new(:bad_request, "Input resource_id must match user id in request")
      end

      if (current_user.id != user_id)
        raise Api::V1::ApiError.new(:forbidden) if !current_org.has_admin_access?(current_user)
        if input[:owner_id].blank? || input[:org_id].blank?
          raise Api::V1::ApiError.new(:bad_request, "Input must include owner_id and org_id when managing settings for another user")
        end
      end
    end

  end
end