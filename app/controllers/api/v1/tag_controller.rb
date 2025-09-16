module Api::V1
  class TagController < Api::V1::ApiController

    def create
      authorize! :manage, @resource
      input = MultiJson.load(request.raw_post)
      ResourceTagging.add_owned_tags(@resource, input, current_user)
      render :template => "api/v1/#{@template_path}/show"
    end

    def update
      authorize! :manage, @resource
      input = MultiJson.load(request.raw_post)
      ResourceTagging.update_owned_tags(@resource, input, current_user)
      render :template => "api/v1/#{@template_path}/show"
    end

    def destroy
      authorize! :manage, @resource
      input = MultiJson.load(request.raw_post)
      ResourceTagging.destroy_owned_tags(@resource, input, current_user)
      render :template => "api/v1/#{@template_path}/show"
    end

    def show
      authorize! :read, @resource
      result = { :tags =>  @resource.tags_list }
      render :json => result
    end

    def setup_tagging_service (model)
      id_param = (model.name.singularize.underscore + "_id").to_sym
      resource = model.find(params[id_param])
      @resource = resource
      @template_path = model.name.underscore.pluralize
      if (@resource.is_a?(DataCredentials))
        @instance_resource = model.name.pluralize.underscore
      else
        @instance_resource = model.name.singularize.underscore
      end
      instance_variable_set("@#{@instance_resource}", @resource)
    end

  end
end