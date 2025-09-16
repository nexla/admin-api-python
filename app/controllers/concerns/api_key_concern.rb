module ApiKeyConcern
  extend ActiveSupport::Concern

  included do |base|
    class << base
      # eg. DataSourcesApiKey
      attr_accessor :api_key_model

      # eg. DataSource
      attr_accessor :resource_model

      # eg. :data_source_id
      attr_accessor :resource_attribute

      def init_api_key_model (controller_name)
        @resource_model = controller_name.camelcase.singularize.constantize
        @api_key_model = (controller_name.camelcase + "ApiKey").constantize
        @resource_attribute = (controller_name.singularize + "_id").to_sym
      end
    end

    base.init_api_key_model(base.controller_name)

    before_action :service_key_data_source!, only: [:create_api_key, :update_api_key, :rotate_api_key, :activate_api_key, :destroy_api_key, :search_api_keys]
  end

  def find_api_key
    @resource = self.class.resource_model.find(params[self.class.resource_attribute])
    authorize! :manage, @resource
    if params[:api_key_id].to_s == 'all'
      api_key = @resource.api_keys
    else
      api_key = if @resource.is_a?(DataSource) && [FlowNode::Flow_Types[:rag], FlowNode::Flow_Types[:api_server]].include?(@resource.flow_type)
        ServiceKey.find(params[:api_key_id])
      else
        self.class.api_key_model.find(params[:api_key_id])
      end
      if (api_key.send(self.class.resource_attribute) != @resource.id)
        raise Api::V1::ApiError.new(:not_found)
      end
    end
    return api_key
  end

  def show_api_keys
    @resource_attribute = self.class.resource_attribute
    @resource = self.class.resource_model.find(params[@resource_attribute])

    authorize! :manage, @resource

    @api_keys = @resource.api_keys.page(@page).per_page(@per_page)
    set_link_header(@api_keys)
    render "show_api_keys"
  end

  def search_api_keys
    @resource_attribute = self.class.resource_attribute
    @resource = self.class.resource_model.find(params[@resource_attribute])

    authorize! :manage, @resource

    scope = @resource.api_keys.page(@page).per_page(@per_page)
    scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, self.class.api_key_model, params[:filters], scope).call
    @api_keys = scope.page(@page).per_page(@per_page)
    render "show_api_keys"
  end

  def show_api_key
    @api_key = find_api_key
    @resource_attribute = self.class.resource_attribute

    if @api_key.is_a?(ServiceKey)
      key_type = 'service_keys'
      @service_key = @api_key
    else
      key_type = 'api_keys'
    end
    render "api/v1/#{key_type}/show"
  end

  def create_api_key
    input = request.raw_post.blank? ? {} : (validate_body_json ApiKey).symbolize_keys
    resource = self.class.resource_model.find(params[self.class.resource_attribute])
    authorize! :manage, resource
    api_user_info = ApiUserInfo.new(current_user, current_org, input)
    @api_key = resource.build_api_key_from_input(api_user_info, input)
    @resource_attribute = self.class.resource_attribute
    render "api/v1/api_keys/show"
  end

  def update_api_key
    input = request.raw_post.blank? ? {} : (validate_body_json ApiKey).symbolize_keys
    @api_key = find_api_key
    api_user_info = ApiUserInfo.new(current_user, current_org, input)
    @api_key.update_mutable!(api_user_info, input)
    @resource_attribute = self.class.resource_attribute
    render "api/v1/api_keys/show"
  end

  def rotate_api_key
    @api_key = find_api_key
    @api_key.rotate!
    @resource_attribute = self.class.resource_attribute
    render "api/v1/api_keys/show"
  end

  def activate_api_key
    is_list = (params[:api_key_id].to_s == 'all')
    @api_key = find_api_key

    if (params[:activate])
      @api_key.activate!
    else
      if (is_list)
        @api_key.each(&:pause!)
      else
        @api_key.pause!
      end
    end

    @resource_attribute = self.class.resource_attribute
    if (is_list)
      render "show_api_keys"
    else
      render "api/v1/api_keys/show"
    end
  end

  def destroy_api_key
    api_key = find_api_key
    api_key.destroy
    head :ok
  end

  protected

  def service_key_data_source!
    if self.class.resource_model == DataSource
      data_source = self.class.resource_model.find(params[self.class.resource_attribute])
      if [FlowNode::Flow_Types[:rag], FlowNode::Flow_Types[:api_server]].include?(data_source.flow_type)
        raise Api::V1::ApiError.new(:method_not_allowed, "Not allowed for RAG and API server flows.")
      end
    end
  end

end
