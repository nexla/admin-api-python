require 'will_paginate/array'

module Api::V1  
  class ValidatorsController < Api::V1::ApiController      

    def public
      @validators = Validator.find_public.paginate(:page => @page, :per_page => @per_page)
      set_link_header(@validators)
      render "index"
    end
   
    def index
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @validators = add_request_filters(
        current_user.validators(current_org, options), CodeContainer
      ).page(@page).per_page(@per_page)

      set_link_header(@validators)
      load_tags(@access_roles)
    end
    
    def show
      return if render_schema CodeContainer
      @validator = Validator.find(params[:id])
      authorize! :read, @validator
    end
    
    def create
      input = (validate_body_json CodeContainer).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @validator = Validator.build_from_input(api_user_info, input)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_validator = CodeContainer.find(params[:validator_id])
      authorize! :manage, copied_validator
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_validator)
      @validator = copied_validator.copy(api_user_info, input)
      render "show"
    end
    
    def update
      input = (validate_body_json CodeContainer).symbolize_keys
      @validator = Validator.find(params[:id])
      authorize! :manage, @validator
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @validator)
      @validator.update_mutable!(api_user_info, input)
      render "show"
    end
    
    def destroy
      validator = Validator.find(params[:id])
      authorize! :manage, validator
      validator.destroy
      head :ok
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @validators = ResourceTagging.search_by_tags(Validator, input, current_user, request_access_role, current_org)
      set_link_header(@validators)
      render "index"
    end

  end
end


