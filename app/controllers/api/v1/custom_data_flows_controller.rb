module Api::V1  
  class CustomDataFlowsController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern

    before_action :initialize_date_params, only: [:metrics] 

    def index
      @custom_data_flows = add_request_filters(
        current_user.custom_data_flows(request_access_role, current_org),
        CustomDataFlow
      ).page(@page).per_page(@per_page)
      set_link_header(@custom_data_flows)
    end

    def show
      return if render_schema CustomDataFlow

      @custom_data_flow = CustomDataFlow.find(params[:id])
      authorize! :read, @custom_data_flow
    end

    def create
      input = (validate_body_json CustomDataFlow).symbolize_keys
      if ((input.key?(:owner) || input.key?(:org)) && !current_user.super_user?)
        raise Api::V1::ApiError.new(:forbidden)
      end
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @custom_data_flow = CustomDataFlow.build_from_input(api_user_info, input, request)
      render "show"
    end

    def copy
      input = (validate_body_json CopyOptions).symbolize_keys if !request.raw_post.blank?
      input ||= {}
      copied_custom_data_flow = CustomDataFlow.find(params[:custom_data_flow_id])
      authorize! :manage, copied_custom_data_flow
      api_user_info = ApiUserInfo.new(current_user, current_org, input, copied_custom_data_flow)
      @custom_data_flow = copied_custom_data_flow.copy(api_user_info, input)
      render "show"
    end

    def update
      input = (validate_body_json CustomDataFlow).symbolize_keys
      if ((input.key?(:owner) || input.key?(:org)) && !current_user.super_user?)
        raise Api::V1::ApiError.new(:forbidden)
      end
      @custom_data_flow = CustomDataFlow.find(params[:id])
      authorize! :manage, @custom_data_flow
      api_user_info = ApiUserInfo.new(current_user, current_org, input, @custom_data_flow)
      @custom_data_flow.update_mutable!(api_user_info, input)
      render "show"
    end

    def destroy
      custom_data_flow = CustomDataFlow.find(params[:id])
      authorize! :manage, custom_data_flow
      custom_data_flow.destroy
      head :ok
    end

    def associations
      @custom_data_flow = CustomDataFlow.find(params[:custom_data_flow_id])

      mode = params[:mode].to_sym
      if mode == :list
        authorize! :read, @custom_data_flow
        ability = nil
      else
        authorize! :manage, @custom_data_flow
        ability = Ability.new(current_user)
      end

      if mode == :remove && request.raw_post.empty?
        # DELETE with no input means delete all associated
        # instances of the given model type.
        assoc_sym = params[:model].name.underscore.pluralize.to_sym
        input = { assoc_sym => [] }
        params[:mode] = :reset
      elsif mode != :list
        input = (validate_body_json CustomDataFlow).symbolize_keys
      end

      begin
        model = params[:model].is_a?(String) ? params[:model].constantize : params[:model]
      rescue StandardError => e
        raise Api::V1::ApiError.new(:bad_request, "Invalid model type: #{params[:model]}")
      end

      case mode
      when :add
        @custom_data_flow.update_associations(model, ability, input)
      when :reset
        @custom_data_flow.reset_associations(model, ability, input)
      when :remove
        @custom_data_flow.remove_associations(model, input)
      end

      render "_show_#{model.name.underscore.pluralize}"
    end

    def activate
      @custom_data_flow = CustomDataFlow.find(params[:custom_data_flow_id])
      authorize! :operate, @custom_data_flow
      params[:activate].truthy? ? @custom_data_flow.activate! : @custom_data_flow.pause!
      render "show"
    end

    def metrics
      @custom_data_flow = CustomDataFlow.find(params[:custom_data_flow_id])
      authorize! :read, @custom_data_flow
      params[:user_id] = current_user.id
      params[:org_id] = current_org.id
      result = MetricsService.new.get_metric_data(@custom_data_flow, params, request)
      render :json => result, :status => result[:status]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @custom_data_flows = ResourceTagging.search_by_tags(CustomDataFlow, input, current_user, request_access_role, current_org)
      set_link_header(@custom_data_flows)
      render "index"
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      scope = current_user.custom_data_flows(request_access_role, current_org)
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, CustomDataFlow, params[:filters], scope, sort_opts: sort_opts).call

      @custom_data_flows = scope.page(@page).per_page(@per_page)
      set_link_header(@custom_data_flows)
      render :index
    end

  end
end


