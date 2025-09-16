module Api::V1  
  class AttributeTransformsController < Api::V1::ApiController      
    include PaperTrailControllerInfo

    before_action only: [:index] do
      if params.key?(:reusable)
        @all = params[:reusable] == "all"
        @reusable = params[:reusable].truthy?
      else
        @all = false
        @reusable = true
      end
    end

    def public
      @attribute_transforms = add_request_filters(AttributeTransform.find_public, CodeContainer).page(@page).per_page(@per_page)
      set_link_header(@attribute_transforms)
      render "index"
    end
   
    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @attribute_transforms = add_request_filters(
        current_user.attribute_transforms(current_org, options), CodeContainer
      ).page(@page).per_page(@per_page)

      unless @all
        @attribute_transforms = @reusable ? 
          @attribute_transforms.reusable : @attribute_transforms.not_reusable
      end

      set_link_header(@attribute_transforms)
      load_tags(@access_roles)
    end
    
    def show
      return if render_schema CodeContainer
      @attribute_transform = AttributeTransform.find(params[:id])
      authorize! :read, @attribute_transform
    end
    
    def create
      input = (validate_body_json CodeContainer).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @attribute_transform = AttributeTransform.build_from_input(api_user_info, input)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_transform = CodeContainer.find(params[:attribute_transform_id])
      authorize! :manage, copied_transform
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_transform)
      @attribute_transform = copied_transform.copy(api_user_info, input)
      render "show"
    end
    
    def update
      input = (validate_body_json CodeContainer).symbolize_keys
      @attribute_transform = AttributeTransform.find(params[:id])
      authorize! :manage, @attribute_transform
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @attribute_transform)
      @attribute_transform.update_mutable!(api_user_info, input)
      render "show"
    end
    
    def destroy
      attribute_transform = AttributeTransform.find(params[:id])
      authorize! :manage, attribute_transform
      attribute_transform.destroy
      head :ok
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @attribute_transforms = ResourceTagging.search_by_tags(AttributeTransform, input, current_user, request_access_role, current_org)
      set_link_header(@attribute_transforms)
      render "index"
    end

  end
end


