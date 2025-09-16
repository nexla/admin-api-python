module Api::V1
  class TransformsController < Api::V1::ApiController
    include TransformConcern
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
      @transforms = add_request_filters(Transform.find_public, CodeContainer).page(@page).per_page(@per_page)
      set_link_header(@transforms)
      render "index"
    end

    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @transforms = add_request_filters(
        current_user.transforms(current_org, options), CodeContainer
      ).page(@page).per_page(@per_page)

      unless @all
        @transforms = @reusable ? @transforms.reusable : @transforms.not_reusable
      end

      set_link_header(@transforms)
      load_tags(@access_roles)
    end

    def show
      return if render_schema CodeContainer
      @transform = Transform.find(params[:id])
      authorize! :read, @transform
    end

    def create
      input = (validate_body_json CodeContainer).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @transform = Transform.build_from_input(api_user_info, input)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_transform = CodeContainer.find(params[:transform_id])
      authorize! :manage, copied_transform
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_transform)
      @transform = copied_transform.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json CodeContainer).symbolize_keys
      @transform = Transform.find(params[:id])
      authorize! :manage, @transform
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @transform)
      @transform.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      transform = Transform.find(params[:id])
      authorize! :manage, transform
      transform.destroy
      head :ok
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @transforms = ResourceTagging.search_by_tags(Transform, input, current_user, request_access_role, current_org)
      set_link_header(@transforms)
      render "index"
    end

  end
end