module Api::V1  
  class DataSchemasController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern

    before_action only: [:index, :index_all, :show] do
      @include_samples = params[:include_samples].truthy?
    end

    def public
      scope = DataSchema.where(:public => true)
      scope = scope.where(:template => params[:template].truthy?) if params.key?(:template)
      @data_schemas = sort_by(scope).page(@page).per_page(@per_page)
      set_link_header(@data_schemas)
      render "index"
    end
      
    def index
      scope = current_user.data_schemas(current_org, access_role: request_access_role, access_roles: @access_roles)
      scope = scope.where(:template => params[:template].truthy?) if params.key?(:template)
      scope = scope.or( DataSchema.public_scope.select( scope.select_values) ) if params[:include_public].truthy?
      @data_schemas = sort_by(scope).page(@page).per_page(@per_page)

      if params[:include_public].truthy?
        @data_schemas.each do |ds|
          if ds.public?
            @access_roles[:data_schemas] ||= {}
            @access_roles[:data_schemas][ds.id] ||= :collaborator
          end
        end
      end

      load_tags(@access_roles)
      set_link_header(@data_schemas)
    end
    
    def index_all
      head :forbidden and return if !current_user.infrastructure_or_super_user?
      @data_schemas = DataSchema.all
      @data_schemas = @data_schemas.in_dataplane(request_dataplane) if request_dataplane.present?     
      @data_schemas = @data_schemas.page(@page).per_page(@per_page)
      set_link_header(@data_schemas)
      render "index"
    end

    def show
      return if render_schema DataSchema
      @data_schema = DataSchema.find(params[:id])
      authorize! :read, @data_schema
    end
    
    def create
      input = (validate_body_json DataSchema).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_schema = DataSchema.build_from_input(api_user_info, input)    
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_data_schema = DataSchema.find(params[:data_schema_id])
      authorize! :manage, copied_data_schema
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_data_schema)
      @data_schema = copied_data_schema.copy(api_user_info, input)
      render "show"
    end
 
    def update
      input = (validate_body_json DataSchema).symbolize_keys

      @data_schema = DataSchema.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_schema.nil?
      authorize! :manage, @data_schema

      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_schema)
      @data_schema.update_mutable!(api_user_info, input, request)
      render "show"
    end

    def destroy
      @data_schema = DataSchema.find(params[:id])
      authorize! :manage, @data_schema
      @data_schema.destroy
      head :ok
    end

    def metrics
      data_schema = DataSchema.find(params[:data_schema_id])
      authorize! :read, data_schema
      data_set = data_schema.data_sets.first
      authorize! :read, data_set
      result = MetricsService.new.get_metric_data(data_set, params)
      render :json => result, :status => result[:status]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_schemas = ResourceTagging.search_by_tags(DataSchema, input, current_user, request_access_role, current_org, params[:public].truthy?)
      set_link_header(@data_schemas)
      render "index"
    end

    def search
      scope = current_user.data_schemas(current_org, access_role: request_access_role, access_roles: @access_roles)
      scope = scope.where(:template => params[:template].truthy?) if params.key?(:template)

      include_public = params[:include_public].truthy?

      sort_opts = params.slice(:sort_by, :sort_order)
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataSchema, params[:filters], scope, include_public: include_public, sort_opts: sort_opts).call
      @data_schemas = scope.page(@page).per_page(@per_page)
      set_link_header(@data_schemas)
      render :index
    end
  end
end


